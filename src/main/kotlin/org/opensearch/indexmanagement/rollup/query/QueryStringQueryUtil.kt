/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.query

import org.apache.lucene.queryparser.classic.ParseException
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.Query
import org.opensearch.common.regex.Regex
import org.opensearch.common.xcontent.LoggingDeprecationHandler
import org.opensearch.index.analysis.NamedAnalyzer
import org.opensearch.index.query.QueryBuilder
import org.opensearch.index.query.QueryShardException
import org.opensearch.index.query.QueryStringQueryBuilder
import org.opensearch.index.query.support.QueryParsers
import org.opensearch.index.search.QueryParserHelper
import org.opensearch.index.search.QueryStringQueryParser
import org.opensearch.indexmanagement.common.model.dimension.Dimension
import org.opensearch.indexmanagement.rollup.util.QueryShardContextFactory

object QueryStringQueryUtil {

    fun rewriteQueryStringQuery(
        queryBuilder: QueryBuilder,
        concreteIndexName: String
    ): QueryStringQueryBuilder {
        val qsqBuilder = queryBuilder as QueryStringQueryBuilder
        val fieldsFromQueryString = extractFieldsV2(queryBuilder, concreteIndexName)
        var newQueryString = qsqBuilder.queryString()
        // Rewrite query_string
        fieldsFromQueryString.forEach { field ->
            newQueryString = newQueryString.replace("$field:", "$field.${Dimension.Type.TERMS.type}:")
        }
        // Rewrite fields
        var newDefaultField: String? = null
        if (qsqBuilder.defaultField() != null) {
            newDefaultField = "${qsqBuilder.defaultField()}.${Dimension.Type.TERMS.type}"
        }
        var newFields: MutableMap<String, Float>? = null
        if (qsqBuilder.fields() != null && qsqBuilder.fields().size > 0) {
            newFields = mutableMapOf()
            qsqBuilder.fields().forEach {
                newFields.put("${it.key}.${Dimension.Type.TERMS.type}", it.value)
            }
        }

        var retVal = QueryStringQueryBuilder(newQueryString)
            .defaultField(newDefaultField)
            .rewrite(qsqBuilder.rewrite())
            .fuzzyRewrite(qsqBuilder.fuzzyRewrite())
            .autoGenerateSynonymsPhraseQuery(qsqBuilder.autoGenerateSynonymsPhraseQuery())
            .allowLeadingWildcard(qsqBuilder.allowLeadingWildcard())
            .analyzeWildcard(qsqBuilder.analyzeWildcard())
            .defaultOperator(qsqBuilder.defaultOperator())
            .escape(qsqBuilder.escape())
            .fuzziness(qsqBuilder.fuzziness())
            .lenient(qsqBuilder.lenient())
            .enablePositionIncrements(qsqBuilder.enablePositionIncrements())
            .fuzzyMaxExpansions(qsqBuilder.fuzzyMaxExpansions())
            .fuzzyPrefixLength(qsqBuilder.fuzzyPrefixLength())
            .queryName(qsqBuilder.queryName())
            .quoteAnalyzer(qsqBuilder.quoteAnalyzer())
            .analyzer(qsqBuilder.analyzer())
            .minimumShouldMatch(qsqBuilder.minimumShouldMatch())
            .timeZone(qsqBuilder.timeZone())
            .phraseSlop(qsqBuilder.phraseSlop())
            .quoteFieldSuffix(qsqBuilder.quoteFieldSuffix())
            .boost(qsqBuilder.boost())
            .fuzzyTranspositions(qsqBuilder.fuzzyTranspositions())

        if (newFields != null && newFields.size > 0) {
            retVal = retVal.fields(newFields)
        }
        if (qsqBuilder.tieBreaker() != null) {
            retVal = retVal.tieBreaker(qsqBuilder.tieBreaker())
        }

        return retVal
    }

    /**
     * Extracts all fields used in QueryStringQueryBuilder
     */
    fun parseQueryStringQueryBuilder(
        queryBuilder: QueryBuilder,
        concreteIndexName: String
    ): Pair<List<String>, String> {

        val aaa = extractFieldsV2(queryBuilder, concreteIndexName)
        println(aaa)
        val fieldsFromQuery = mutableListOf<String>()
        val context = QueryShardContextFactory.createShardContext(concreteIndexName)
        val luceneQuery = queryBuilder.toQuery(context)

        val queryParser: QueryStringQueryParser
        val isLenient = context.queryStringLenient()
        val defaultFields: List<String> = context.defaultFields()
        if (QueryParserHelper.hasAllFieldsWildcard(defaultFields)) {
            queryParser = object : QueryStringQueryParser(context, false) {
                override fun getFuzzyQuery(field: String?, termStr: String?, minSimilarity: Float): Query? {
                    if (field != null) fieldsFromQuery.add(field)
                    return super.getFuzzyQuery(field, termStr, minSimilarity)
                }
                override fun getPrefixQuery(field: String?, termStr: String?): Query {
                    if (field != null) fieldsFromQuery.add(field)
                    return super.getPrefixQuery(field, termStr)
                }
                override fun getFieldQuery(field: String?, queryText: String?, quoted: Boolean): Query {
                    if (field != null) fieldsFromQuery.add(field)
                    return super.getFieldQuery(field, queryText, quoted)
                }
                override fun getWildcardQuery(field: String?, termStr: String?): Query {
                    if (field != null) fieldsFromQuery.add(field)
                    return super.getWildcardQuery(field, termStr)
                }
                override fun getFieldQuery(field: String?, queryText: String?, slop: Int): Query {
                    if (field != null) fieldsFromQuery.add(field)
                    return super.getFieldQuery(field, queryText, slop)
                }
                override fun getRangeQuery(field: String?, part1: String?, part2: String?, startInclusive: Boolean, endInclusive: Boolean): Query {
                    if (field != null) fieldsFromQuery.add(field)
                    return super.getRangeQuery(field, part1, part2, startInclusive, endInclusive)
                }
            }
        } else {
            val resolvedFields = QueryParserHelper.resolveMappingFields(
                context,
                QueryParserHelper.parseFieldsAndWeights(defaultFields)
            )
            queryParser = object : QueryStringQueryParser(context, resolvedFields, isLenient) {
                override fun getFuzzyQuery(field: String?, termStr: String?, minSimilarity: Float): Query? {
                    if (field != null) fieldsFromQuery.add(field)
                    return super.getFuzzyQuery(field, termStr, minSimilarity)
                }
                override fun getPrefixQuery(field: String?, termStr: String?): Query {
                    if (field != null) fieldsFromQuery.add(field)
                    return super.getPrefixQuery(field, termStr)
                }
                override fun getFieldQuery(field: String?, queryText: String?, quoted: Boolean): Query {
                    if (field != null) fieldsFromQuery.add(field)
                    return super.getFieldQuery(field, queryText, quoted)
                }
                override fun getWildcardQuery(field: String?, termStr: String?): Query {
                    if (field != null) fieldsFromQuery.add(field)
                    return super.getWildcardQuery(field, termStr)
                }
                override fun getFieldQuery(field: String?, queryText: String?, slop: Int): Query {
                    if (field != null) fieldsFromQuery.add(field)
                    return super.getFieldQuery(field, queryText, slop)
                }
                override fun getRangeQuery(field: String?, part1: String?, part2: String?, startInclusive: Boolean, endInclusive: Boolean): Query {
                    if (field != null) fieldsFromQuery.add(field)
                    return super.getRangeQuery(field, part1, part2, startInclusive, endInclusive)
                }
            }
        }
        var parsedQuery: Query?
        try {
            parsedQuery = queryParser.parse(luceneQuery.toString())
        } catch (e: Exception) {
            throw IllegalArgumentException("The ${queryBuilder.name} query is invalid")
        }
        return fieldsFromQuery to parsedQuery.toString()
    }

    fun extractFieldsV2(queryBuilder: QueryBuilder, concreteIndexName: String): List<String> {
        val context = QueryShardContextFactory.createShardContext(concreteIndexName)
        val qsqBuilder = queryBuilder as QueryStringQueryBuilder
        val rewrittenQueryString = if (qsqBuilder.escape()) QueryParser.escape(qsqBuilder.queryString()) else qsqBuilder.queryString()
        val queryParser: QueryStringQueryParserExt
        val isLenient: Boolean = if (qsqBuilder.lenient() == null) context.queryStringLenient() else qsqBuilder.lenient()
        if (qsqBuilder.defaultField() != null) {
            if (Regex.isMatchAllPattern(qsqBuilder.defaultField())) {
                queryParser = QueryStringQueryParserExt(context, if (qsqBuilder.lenient() == null) true else qsqBuilder.lenient())
            } else {
                queryParser = QueryStringQueryParserExt(context, qsqBuilder.defaultField(), isLenient)
            }
        } else if (qsqBuilder.fields().size > 0) {
            val resolvedFields = QueryParserHelper.resolveMappingFields(context, qsqBuilder.fields())
            queryParser = if (QueryParserHelper.hasAllFieldsWildcard(qsqBuilder.fields().keys)) {
                QueryStringQueryParserExt(context, resolvedFields, if (qsqBuilder.lenient() == null) true else qsqBuilder.lenient())
            } else {
                QueryStringQueryParserExt(context, resolvedFields, isLenient)
            }
        } else {
            val defaultFields: List<String> = context.defaultFields()
            queryParser = if (QueryParserHelper.hasAllFieldsWildcard(defaultFields)) {
                QueryStringQueryParserExt(context, if (qsqBuilder.lenient() == null) true else qsqBuilder.lenient())
            } else {
                val resolvedFields = QueryParserHelper.resolveMappingFields(
                    context,
                    QueryParserHelper.parseFieldsAndWeights(defaultFields)
                )
                QueryStringQueryParserExt(context, resolvedFields, isLenient)
            }
        }

        if (qsqBuilder.analyzer() != null) {
            val namedAnalyzer: NamedAnalyzer = context.getIndexAnalyzers().get(qsqBuilder.analyzer())
                ?: throw QueryShardException(context, "[query_string] analyzer [$qsqBuilder.analyzer] not found")
            queryParser.setForceAnalyzer(namedAnalyzer)
        }

        if (qsqBuilder.quoteAnalyzer() != null) {
            val forceQuoteAnalyzer: NamedAnalyzer = context.getIndexAnalyzers().get(qsqBuilder.quoteAnalyzer())
                ?: throw QueryShardException(context, "[query_string] quote_analyzer [$qsqBuilder.quoteAnalyzer] not found")
            queryParser.setForceQuoteAnalyzer(forceQuoteAnalyzer)
        }

        queryParser.defaultOperator = qsqBuilder.defaultOperator().toQueryParserOperator()
        // TODO can we extract this somehow? There's no getter for this
        queryParser.setType(QueryStringQueryBuilder.DEFAULT_TYPE)
        if (qsqBuilder.tieBreaker() != null) {
            queryParser.setGroupTieBreaker(qsqBuilder.tieBreaker())
        } else {
            queryParser.setGroupTieBreaker(QueryStringQueryBuilder.DEFAULT_TYPE.tieBreaker())
        }
        queryParser.phraseSlop = qsqBuilder.phraseSlop()
        queryParser.setQuoteFieldSuffix(qsqBuilder.quoteFieldSuffix())
        queryParser.setAllowLeadingWildcard(
            if (qsqBuilder.allowLeadingWildcard() == null) context.queryStringAllowLeadingWildcard() else qsqBuilder.allowLeadingWildcard()
        )
        queryParser.setAnalyzeWildcard(if (qsqBuilder.analyzeWildcard() == null) context.queryStringAnalyzeWildcard() else qsqBuilder.analyzeWildcard())
        queryParser.enablePositionIncrements = qsqBuilder.enablePositionIncrements()
        queryParser.setFuzziness(qsqBuilder.fuzziness())
        queryParser.fuzzyPrefixLength = qsqBuilder.fuzzyPrefixLength()
        queryParser.setFuzzyMaxExpansions(qsqBuilder.fuzzyMaxExpansions())
        queryParser.setFuzzyRewriteMethod(QueryParsers.parseRewriteMethod(qsqBuilder.fuzzyRewrite(), LoggingDeprecationHandler.INSTANCE))
        queryParser.multiTermRewriteMethod = QueryParsers.parseRewriteMethod(qsqBuilder.rewrite(), LoggingDeprecationHandler.INSTANCE)
        queryParser.setTimeZone(qsqBuilder.timeZone())
        queryParser.determinizeWorkLimit = qsqBuilder.maxDeterminizedStates()
        queryParser.autoGenerateMultiTermSynonymsPhraseQuery = qsqBuilder.autoGenerateSynonymsPhraseQuery()
        queryParser.setFuzzyTranspositions(qsqBuilder.fuzzyTranspositions())

        try {
            queryParser.parse(rewrittenQueryString)
        } catch (e: ParseException) {
            throw QueryShardException(context, "Failed to parse query [" + qsqBuilder.queryString() + "]", e)
        }

        return queryParser.getAllDiscoveredFields()
    }
}
