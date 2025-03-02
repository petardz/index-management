/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indexmanagement.indexstatemanagement.model.destination

import org.opensearch.core.common.Strings
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.io.stream.Writeable
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.core.xcontent.XContentParser.Token
import org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import java.io.IOException

/**
 * A value object that represents a Custom webhook message. Webhook message will be
 * submitted to the Custom webhook destination
 *
 * Temporary import from alerting, this will be removed once we pull notifications out of
 * alerting so all plugins can consume and use.
 */
data class CustomWebhook(
    val url: String?,
    val scheme: String?,
    val host: String?,
    val port: Int,
    val path: String?,
    val queryParams: Map<String, String>,
    val headerParams: Map<String, String>,
    val username: String?,
    val password: String?
) : ToXContent, Writeable {

    init {
        require(!(Strings.isNullOrEmpty(url) && Strings.isNullOrEmpty(host))) {
            "Url or Host name must be provided."
        }
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject(TYPE)
            .field(URL, url)
            .field(SCHEME_FIELD, scheme)
            .field(HOST_FIELD, host)
            .field(PORT_FIELD, port)
            .field(PATH_FIELD, path)
            .field(QUERY_PARAMS_FIELD, queryParams)
            .field(HEADER_PARAMS_FIELD, headerParams)
            .field(USERNAME_FIELD, username)
            .field(PASSWORD_FIELD, password)
            .endObject()
    }

    constructor(sin: StreamInput) : this(
        sin.readOptionalString(),
        sin.readOptionalString(),
        sin.readOptionalString(),
        sin.readInt(),
        sin.readOptionalString(),
        suppressWarning(sin.readMap()),
        suppressWarning(sin.readMap()),
        sin.readOptionalString(),
        sin.readOptionalString()
    )

    override fun writeTo(out: StreamOutput) {
        out.writeOptionalString(url)
        out.writeOptionalString(scheme)
        out.writeOptionalString(host)
        out.writeInt(port)
        out.writeOptionalString(path)
        out.writeMap(queryParams)
        out.writeMap(headerParams)
        out.writeOptionalString(username)
        out.writeOptionalString(password)
    }

    companion object {
        const val URL = "url"
        const val TYPE = "custom_webhook"
        const val SCHEME_FIELD = "scheme"
        const val HOST_FIELD = "host"
        const val PORT_FIELD = "port"
        const val PATH_FIELD = "path"
        const val QUERY_PARAMS_FIELD = "query_params"
        const val HEADER_PARAMS_FIELD = "header_params"
        const val USERNAME_FIELD = "username"
        const val PASSWORD_FIELD = "password"

        @Suppress("ComplexMethod")
        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): CustomWebhook {
            var url: String? = null
            var scheme: String? = null
            var host: String? = null
            var port: Int = -1
            var path: String? = null
            var queryParams: Map<String, String> = mutableMapOf()
            var headerParams: Map<String, String> = mutableMapOf()
            var username: String? = null
            var password: String? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    URL -> url = xcp.textOrNull()
                    SCHEME_FIELD -> scheme = xcp.textOrNull()
                    HOST_FIELD -> host = xcp.textOrNull()
                    PORT_FIELD -> port = xcp.intValue()
                    PATH_FIELD -> path = xcp.textOrNull()
                    QUERY_PARAMS_FIELD -> queryParams = xcp.mapStrings()
                    HEADER_PARAMS_FIELD -> headerParams = xcp.mapStrings()
                    USERNAME_FIELD -> username = xcp.textOrNull()
                    PASSWORD_FIELD -> password = xcp.textOrNull()
                    else -> {
                        error("Unexpected field: $fieldName, while parsing custom webhook destination")
                    }
                }
            }
            return CustomWebhook(url, scheme, host, port, path, queryParams, headerParams, username, password)
        }

        @Suppress("UNCHECKED_CAST")
        fun suppressWarning(map: MutableMap<String?, Any?>?): MutableMap<String, String> {
            return map as MutableMap<String, String>
        }
    }
}
