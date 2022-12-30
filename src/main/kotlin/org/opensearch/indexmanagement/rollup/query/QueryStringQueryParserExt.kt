/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.rollup.query

import org.apache.lucene.search.Query
import org.opensearch.index.query.QueryShardContext
import org.opensearch.index.search.QueryStringQueryParser

class QueryStringQueryParserExt : QueryStringQueryParser {

    val discoveredFields = mutableListOf<String>()

    constructor(context: QueryShardContext?, lenient: Boolean) : super(context, lenient) {
    }

    constructor(context: QueryShardContext?, defaultField: String, lenient: Boolean) : super(context, defaultField, lenient) {
    }

    constructor(context: QueryShardContext, resolvedFields: Map<String, Float>, lenient: Boolean) : super(context, resolvedFields, lenient) {
    }

    override fun getFuzzyQuery(field: String?, termStr: String?, minSimilarity: Float): Query? {
        if (field != null) discoveredFields.add(field)
        return super.getFuzzyQuery(field, termStr, minSimilarity)
    }
    override fun getPrefixQuery(field: String?, termStr: String?): Query {
        if (field != null) discoveredFields.add(field)
        return super.getPrefixQuery(field, termStr)
    }
    override fun getFieldQuery(field: String?, queryText: String?, quoted: Boolean): Query {
        if (field != null) discoveredFields.add(field)
        return super.getFieldQuery(field, queryText, quoted)
    }
    override fun getWildcardQuery(field: String?, termStr: String?): Query {
        if (field != null) discoveredFields.add(field)
        return super.getWildcardQuery(field, termStr)
    }
    override fun getFieldQuery(field: String?, queryText: String?, slop: Int): Query {
        if (field != null) discoveredFields.add(field)
        return super.getFieldQuery(field, queryText, slop)
    }
    override fun getRangeQuery(field: String?, part1: String?, part2: String?, startInclusive: Boolean, endInclusive: Boolean): Query {
        if (field != null) discoveredFields.add(field)
        return super.getRangeQuery(field, part1, part2, startInclusive, endInclusive)
    }

    fun getAllDiscoveredFields(): MutableList<String> {
        return this.discoveredFields
    }
}
