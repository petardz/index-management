/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.transform

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.WriteRequest
import org.opensearch.client.Client
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.NamedXContentRegistry
import org.opensearch.index.shard.ShardId
import org.opensearch.indexmanagement.opensearchapi.IndexManagementSecurityContext
import org.opensearch.indexmanagement.opensearchapi.suspendUntil
import org.opensearch.indexmanagement.opensearchapi.withClosableContext
import org.opensearch.indexmanagement.transform.action.index.IndexTransformAction
import org.opensearch.indexmanagement.transform.action.index.IndexTransformRequest
import org.opensearch.indexmanagement.transform.action.index.IndexTransformResponse
import org.opensearch.indexmanagement.transform.model.BucketsToTransform
import org.opensearch.indexmanagement.transform.model.ContinuousTransformStats
import org.opensearch.indexmanagement.transform.model.ShardNewDocuments
import org.opensearch.indexmanagement.transform.model.Transform
import org.opensearch.indexmanagement.transform.model.TransformMetadata
import org.opensearch.indexmanagement.transform.model.initializeShardsToSearch
import org.opensearch.indexmanagement.transform.util.TransformContext
import org.opensearch.indexmanagement.transform.util.TransformLockManager
import org.opensearch.jobscheduler.spi.JobExecutionContext
import org.opensearch.jobscheduler.spi.ScheduledJobParameter
import org.opensearch.jobscheduler.spi.ScheduledJobRunner
import org.opensearch.monitor.jvm.JvmService
import org.opensearch.threadpool.ThreadPool
import java.time.Instant

@Suppress("LongParameterList", "TooManyFunctions")
object TransformRunner :
    ScheduledJobRunner,
    CoroutineScope by CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("TransformRunner")) {

    private val logger = LogManager.getLogger(javaClass)

    private lateinit var client: Client
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var clusterService: ClusterService
    private lateinit var settings: Settings
    private lateinit var transformMetadataService: TransformMetadataService
    private lateinit var transformSearchService: TransformSearchService
    private lateinit var transformIndexer: TransformIndexer
    private lateinit var transformValidator: TransformValidator
    private lateinit var threadPool: ThreadPool

    fun initialize(
        client: Client,
        clusterService: ClusterService,
        xContentRegistry: NamedXContentRegistry,
        settings: Settings,
        indexNameExpressionResolver: IndexNameExpressionResolver,
        jvmService: JvmService,
        threadPool: ThreadPool
    ): TransformRunner {
        this.clusterService = clusterService
        this.client = client
        this.xContentRegistry = xContentRegistry
        this.settings = settings
        this.transformSearchService = TransformSearchService(settings, clusterService, client)
        this.transformMetadataService = TransformMetadataService(client, xContentRegistry)
        this.transformIndexer = TransformIndexer(settings, clusterService, client)
        this.transformValidator = TransformValidator(indexNameExpressionResolver, clusterService, client, settings, jvmService)
        this.threadPool = threadPool
        return this
    }

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        if (job !is Transform) {
            throw IllegalArgumentException("Received invalid job type [${job.javaClass.simpleName}] with id [${context.jobId}]")
        }

        launch {
            try {
                if (job.enabled) {
                    val metadata = transformMetadataService.getMetadata(job)
                    var transform = job
                    if (job.metadataId == null) {
                        transform = updateTransform(job.copy(metadataId = metadata.id))
                    }
                    if (job.continuous) {
                        executeContinuousJob(transform, metadata, context)
                    } else {
                        executeSingleShotJob(transform, metadata, context)
                    }
                }
            } catch (e: Exception) {
                logger.error("Failed to run job [${job.id}] because ${e.localizedMessage}", e)
                return@launch
            }
        }
    }
    /*
    * 1. index stats request -> extract list of ShardNewDocuments: shardId, oldSeqNo, newSeqNo
    * 2. Send N search requests to N shards. Maintain N running coroutines sending requests
    * 3. Merge search results to bucket log and send message to recompute channelhistory
    * 4. Recompute Engine - maintain M coroutines executing bucket recompute
    * 5. on new ch msg: fetch maxPageSize buckets from bucket log and execute recompute
    * */
    // TODO: Add circuit breaker checks - [cluster healthy, utilization within limit]
/*    @Suppress("NestedBlockDepth", "ComplexMethod", "LongMethod", "ReturnCount")
    private suspend fun executeJob(transform: Transform, metadata: TransformMetadata, context: JobExecutionContext) {
        var newGlobalCheckpoints: Map<ShardId, Long>? = null
        var newGlobalCheckpointTime: Instant? = null
        var currentMetadata = metadata

        val transformProcessedBucketLog = TransformBucketsStore()
        var bucketsToTransform = BucketsToTransform(HashSet(), metadata)

        val transformContext = TransformContext(TransformLockManager(transform, context))
        // Acquires the lock if there is no running job execution for the given transform; Lock is acquired per transform
        val transformLockManager = transformContext.transformLockManager
        transformLockManager.acquireLockForScheduledJob()
        try {
            do {
                when {
                    transformLockManager.lock == null -> {
                        logger.warn("Cannot acquire lock for transform job ${transform.id}")
                        return
                    }
                    listOf(TransformMetadata.Status.STOPPED, TransformMetadata.Status.FINISHED).contains(metadata.status) -> {
                        logger.warn("Transform job ${transform.id} is in ${metadata.status} status. Skipping execution")
                        return
                    }
                    else -> {
                        val validatedMetadata = validateTransform(transform, currentMetadata)
                        if (validatedMetadata.status == TransformMetadata.Status.FAILED) {
                            currentMetadata = validatedMetadata
                            return
                        }
                        if (transform.continuous) {
                            // If we have not populated the list of shards to search, do so now
                            if (bucketsToTransform.shardsToSearch == null) {
                                // Note the timestamp when we got the shard global checkpoints to the user may know what data is included
                                newGlobalCheckpointTime = Instant.now()
                                newGlobalCheckpoints = transformSearchService.getShardsGlobalCheckpoint(transform.sourceIndex)
                                bucketsToTransform = bucketsToTransform.initializeShardsToSearch(
                                    metadata.shardIDToGlobalCheckpoint,
                                    newGlobalCheckpoints
                                )
                            }
                            // If there are shards to search do it here
                            if (bucketsToTransform.currentShard != null) {
                                // Computes aggregation on modified documents for current shard to get modified buckets
                                bucketsToTransform = getBucketsToTransformIteration(transform, bucketsToTransform, transformContext).also {
                                    currentMetadata = it.metadata
                                }
                                // Filter out already processed buckets
                                val modifiedBuckets = bucketsToTransform.modifiedBuckets.filter {
                                    transformProcessedBucketLog.isNotProcessed(it)
                                }
                                // Recompute modified buckets and update them in targetIndex
                                currentMetadata = recomputeModifiedBuckets(transform, currentMetadata, modifiedBuckets, transformContext)
                                // Add processed buckets to 'processed set' so that we don't try to reprocess them again
                                transformProcessedBucketLog.addBuckets(modifiedBuckets)
                                // Update TransformMetadata
                                currentMetadata = transformMetadataService.writeMetadata(currentMetadata, true)
                                bucketsToTransform = bucketsToTransform.copy(metadata = currentMetadata)
                            }
                        } else {
                            // Computes buckets from source index and stores them in targetIndex as transform docs
                            currentMetadata = computeBucketsIteration(transform, currentMetadata, transformContext)
                            // Update TransformMetadata
                            currentMetadata = transformMetadataService.writeMetadata(currentMetadata, true)
                        }
                        // we attempt to renew lock for every loop of transform
                        transformLockManager.renewLockForScheduledJob()
                    }
                }
            } while (bucketsToTransform.currentShard != null || currentMetadata.afterKey != null)
        } catch (e: Exception) {
            logger.error("Failed to execute the transform job [${transform.id}] because of exception [${e.localizedMessage}]", e)
            currentMetadata = currentMetadata.copy(
                lastUpdatedAt = Instant.now(),
                status = TransformMetadata.Status.FAILED,
                failureReason = e.localizedMessage
            )
        } finally {
            transformLockManager.lock?.let {
                // Update the global checkpoints only after execution finishes successfully
                if (transform.continuous && currentMetadata.status != TransformMetadata.Status.FAILED) {
                    currentMetadata = currentMetadata.copy(
                        shardIDToGlobalCheckpoint = newGlobalCheckpoints,
                        continuousStats = ContinuousTransformStats(newGlobalCheckpointTime, null)
                    )
                }
                transformMetadataService.writeMetadata(currentMetadata, true)
                if (!transform.continuous || currentMetadata.status == TransformMetadata.Status.FAILED) {
                    logger.info("Disabling the transform job ${transform.id}")
                    updateTransform(transform.copy(enabled = false, enabledAt = null))
                }
                transformLockManager.releaseLockForScheduledJob()
            }
        }
    }
*/
    private suspend fun executeContinuousJob(transform: Transform, metadata: TransformMetadata, context: JobExecutionContext) {
        var newGlobalCheckpoints: MutableMap<ShardId, Long>? = null
        var newGlobalCheckpointTime: Instant? = null
        var currentMetadata = metadata

        var bucketsToTransform = BucketsToTransform(HashSet(), metadata)

        val transformContext = TransformContext(TransformLockManager(transform, context))
        // Acquires the lock if there is no running job execution for the given transform; Lock is acquired per transform
        val transformLockManager = transformContext.transformLockManager
        transformLockManager.acquireLockForScheduledJob()
        try {
            when {
                transformLockManager.lock == null -> {
                    logger.warn("Cannot acquire lock for transform job ${transform.id}")
                    return
                }
                listOf(TransformMetadata.Status.STOPPED, TransformMetadata.Status.FINISHED).contains(metadata.status) -> {
                    logger.warn("Transform job ${transform.id} is in ${metadata.status} status. Skipping execution")
                    return
                }
                else -> {
                    val validatedMetadata = validateTransform(transform, currentMetadata)
                    if (validatedMetadata.status == TransformMetadata.Status.FAILED) {
                        currentMetadata = validatedMetadata
                        return
                    }
                    // Note the timestamp when we got the shard global checkpoints to the user may know what data is included
                    newGlobalCheckpointTime = Instant.now()
                    newGlobalCheckpoints = transformSearchService.getShardsGlobalCheckpoint(transform.sourceIndex)
                    val shardsToSearch = bucketsToTransform.initializeShardsToSearch(
                        metadata.shardIDToGlobalCheckpoint,
                        newGlobalCheckpoints
                    )
                    val transformModifiedBucketsFetcher = TransformModifiedBucketsFetcher(
                        client = client,
                        shardNewDocumentsList = shardsToSearch as MutableList<ShardNewDocuments>,
                        transform = transform,
                        transformMetadata = currentMetadata,
                        transformContext = transformContext,
                        transformSearchService = transformSearchService,
                        transformMetadataService = transformMetadataService
                    )
                    val (modifiedBuckets, failedShards, updatedMetadata) = transformModifiedBucketsFetcher.fetchAllModifiedBuckets()

                    currentMetadata = updatedMetadata
                    // remove all failed shards from checkpoint
                    failedShards.forEach { newGlobalCheckpoints.remove(it) }
                    // recompute modified buckets from scratch
                    currentMetadata = recomputeModifiedBuckets(transform, currentMetadata, modifiedBuckets, transformContext)
                    // we attempt to renew lock for every loop of transform
                    transformLockManager.renewLockForScheduledJob()
                }
            }
        } catch (e: Exception) {
            logger.error("Failed to execute the transform job [${transform.id}] because of exception [${e.localizedMessage}]", e)
            currentMetadata = currentMetadata.copy(
                lastUpdatedAt = Instant.now(),
                status = TransformMetadata.Status.FAILED,
                failureReason = e.localizedMessage
            )
        } finally {
            transformLockManager.lock?.let {
                // Update the global checkpoints only after execution finishes successfully
                if (currentMetadata.status != TransformMetadata.Status.FAILED) {
                    currentMetadata = currentMetadata.copy(
                        shardIDToGlobalCheckpoint = newGlobalCheckpoints,
                        continuousStats = ContinuousTransformStats(newGlobalCheckpointTime, null)
                    )
                }
                transformMetadataService.writeMetadata(currentMetadata, true)
                if (currentMetadata.status == TransformMetadata.Status.FAILED) {
                    logger.info("Disabling the transform job ${transform.id}")
                    updateTransform(transform.copy(enabled = false, enabledAt = null))
                }
                transformLockManager.releaseLockForScheduledJob()
            }
        }
    }

    private suspend fun executeSingleShotJob(transform: Transform, metadata: TransformMetadata, context: JobExecutionContext) {
        var currentMetadata = metadata

        val transformContext = TransformContext(TransformLockManager(transform, context))
        // Acquires the lock if there is no running job execution for the given transform; Lock is acquired per transform
        val transformLockManager = transformContext.transformLockManager
        transformLockManager.acquireLockForScheduledJob()
        try {
            do {
                when {
                    transformLockManager.lock == null -> {
                        logger.warn("Cannot acquire lock for transform job ${transform.id}")
                        return
                    }
                    listOf(TransformMetadata.Status.STOPPED, TransformMetadata.Status.FINISHED).contains(metadata.status) -> {
                        logger.warn("Transform job ${transform.id} is in ${metadata.status} status. Skipping execution")
                        return
                    }
                    else -> {
                        val validatedMetadata = validateTransform(transform, currentMetadata)
                        if (validatedMetadata.status == TransformMetadata.Status.FAILED) {
                            currentMetadata = validatedMetadata
                            return
                        }
                        // Computes buckets from source index and stores them in targetIndex as transform docs
                        currentMetadata = computeBucketsIteration(transform, currentMetadata, transformContext)
                        // Update TransformMetadata
                        currentMetadata = transformMetadataService.writeMetadata(currentMetadata, true)
                        // we attempt to renew lock for every loop of transform
                        transformLockManager.renewLockForScheduledJob()
                    }
                }
            } while (currentMetadata.afterKey != null)
        } catch (e: Exception) {
            logger.error("Failed to execute the transform job [${transform.id}] because of exception [${e.localizedMessage}]", e)
            currentMetadata = currentMetadata.copy(
                lastUpdatedAt = Instant.now(),
                status = TransformMetadata.Status.FAILED,
                failureReason = e.localizedMessage
            )
        } finally {
            transformLockManager.lock?.let {
                transformMetadataService.writeMetadata(currentMetadata, true)
                logger.info("Disabling the transform job ${transform.id}")
                updateTransform(transform.copy(enabled = false, enabledAt = null))
                transformLockManager.releaseLockForScheduledJob()
            }
        }
    }

    private suspend fun getBucketsToTransformIteration(
        transform: Transform,
        bucketsToTransform: BucketsToTransform,
        transformContext: TransformContext
    ): BucketsToTransform {
        var currentBucketsToTransform = bucketsToTransform
        val currentShard = bucketsToTransform.currentShard
        // Clear modified buckets from previous iteration
        currentBucketsToTransform.modifiedBuckets.clear()

        if (currentShard != null) {
            val shardLevelModifiedBuckets = withTransformSecurityContext(transform) {
                transformSearchService.getShardLevelModifiedBuckets(
                    transform,
                    currentBucketsToTransform.metadata.afterKey,
                    currentShard,
                    transformContext
                )
            }
            currentBucketsToTransform.modifiedBuckets.addAll(shardLevelModifiedBuckets.modifiedBuckets)
            val mergedSearchTime = currentBucketsToTransform.metadata.stats.searchTimeInMillis +
                shardLevelModifiedBuckets.searchTimeInMillis
            currentBucketsToTransform = currentBucketsToTransform.copy(
                metadata = currentBucketsToTransform.metadata.copy(
                    stats = currentBucketsToTransform.metadata.stats.copy(
                        pagesProcessed = currentBucketsToTransform.metadata.stats.pagesProcessed + 1,
                        searchTimeInMillis = mergedSearchTime
                    ),
                    afterKey = shardLevelModifiedBuckets.afterKey
                ),
                currentShard = currentShard
            )
        }
        // If finished with this shard, go to the next
        if (currentBucketsToTransform.metadata.afterKey == null) {
            val shardsToSearch = currentBucketsToTransform.shardsToSearch
            currentBucketsToTransform = if (shardsToSearch?.hasNext() == true) {
                currentBucketsToTransform.copy(currentShard = shardsToSearch.next())
            } else {
                currentBucketsToTransform.copy(currentShard = null)
            }
        }
        return currentBucketsToTransform
    }

    private suspend fun validateTransform(transform: Transform, transformMetadata: TransformMetadata): TransformMetadata {
        val validationResult = withTransformSecurityContext(transform) {
            transformValidator.validate(transform)
        }
        return if (!validationResult.isValid) {
            val failureMessage = "Failed validation - ${validationResult.issues}"
            val failureMetadata = transformMetadata.copy(status = TransformMetadata.Status.FAILED, failureReason = failureMessage)
            transformMetadataService.writeMetadata(failureMetadata, true)
        } else transformMetadata
    }

    /**
     * For a continuous transform, we paginate over the set of modified buckets, however, with a histogram grouping and a decimal interval,
     * the range query will not precisely specify the modified buckets. As a result, we increase the range for the query and then filter out
     * the unintended buckets as part of the composite search step.
     */
    private suspend fun computeBucketsIteration(
        transform: Transform,
        metadata: TransformMetadata,
        transformContext: TransformContext
    ): TransformMetadata {

        val transformSearchResult = withTransformSecurityContext(transform) {
            transformSearchService.executeCompositeSearch(
                transform,
                metadata.afterKey,
                null,
                transformContext
            )
        }
        val indexTimeInMillis = withTransformSecurityContext(transform) {
            transformIndexer.index(transformSearchResult.docsToIndex)
        }
        val afterKey = transformSearchResult.afterKey
        val stats = transformSearchResult.stats
        val updatedStats = stats.copy(
            pagesProcessed = stats.pagesProcessed,
            indexTimeInMillis = stats.indexTimeInMillis + indexTimeInMillis,
            documentsIndexed = transformSearchResult.docsToIndex.size.toLong()
        )
        return metadata.mergeStats(updatedStats).copy(
            afterKey = afterKey,
            lastUpdatedAt = Instant.now(),
            status = if (afterKey == null) TransformMetadata.Status.FINISHED else TransformMetadata.Status.STARTED
        )
    }

    private suspend fun recomputeModifiedBuckets(
        transform: Transform,
        metadata: TransformMetadata,
        modifiedBuckets: List<Map<String, Any>>,
        transformContext: TransformContext
    ): TransformMetadata {
        val updatedMetadata = if (modifiedBuckets.isNotEmpty()) {
            val maxPageSize = transformSearchService.getMaxRecomputePageSize(transform)
            var currentMetadata = metadata
            for (i in modifiedBuckets.indices step maxPageSize) {
                val end =
                    if (i + maxPageSize <= modifiedBuckets.size) i + maxPageSize
                    else modifiedBuckets.size
                val iterBuckets = modifiedBuckets.subList(
                    i,
                    end
                )
                val transformSearchResult = withTransformSecurityContext(transform) {
                    transformSearchService.executeCompositeSearch(transform, null, iterBuckets, transformContext)
                }
                val indexTimeInMillis = withTransformSecurityContext(transform) {
                    transformIndexer.index(transformSearchResult.docsToIndex)
                }
                val stats = transformSearchResult.stats
                val updatedStats = stats.copy(
                    pagesProcessed = if (transform.continuous) 0 else stats.pagesProcessed,
                    indexTimeInMillis = stats.indexTimeInMillis + indexTimeInMillis,
                    documentsIndexed = transformSearchResult.docsToIndex.size.toLong()
                )
                currentMetadata = currentMetadata.mergeStats(updatedStats).copy(
                    lastUpdatedAt = Instant.now(),
                    status = TransformMetadata.Status.STARTED
                )
                currentMetadata = transformMetadataService.writeMetadata(currentMetadata, true)
            }
            currentMetadata
        } else metadata.copy(lastUpdatedAt = Instant.now(), status = TransformMetadata.Status.STARTED)
        return updatedMetadata
    }

    private suspend fun <T> withTransformSecurityContext(transform: Transform, block: suspend CoroutineScope.() -> T): T {
        return withClosableContext(IndexManagementSecurityContext(transform.id, settings, threadPool.threadContext, transform.user), block)
    }

    private suspend fun updateTransform(transform: Transform): Transform {
        val request = IndexTransformRequest(
            transform = transform.copy(updatedAt = Instant.now()),
            refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE
        )
        return withClosableContext(
            IndexManagementSecurityContext(transform.id, settings, threadPool.threadContext, null)
        ) {
            val response: IndexTransformResponse = client.suspendUntil {
                execute(IndexTransformAction.INSTANCE, request, it)
            }
            return@withClosableContext transform.copy(
                seqNo = response.seqNo,
                primaryTerm = response.primaryTerm
            )
        }
    }
}
