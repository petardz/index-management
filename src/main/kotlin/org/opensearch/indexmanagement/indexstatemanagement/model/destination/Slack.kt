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
 * A value object that represents a Slack message. Slack message will be
 * submitted to the Slack destination
 *
 * Temporary import from alerting, this will be removed once we pull notifications out of
 * alerting so all plugins can consume and use.
 */
data class Slack(val url: String) : ToXContent, Writeable {

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject(TYPE)
            .field(URL, url)
            .endObject()
    }

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        sin.readString()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeString(url)
    }

    companion object {
        const val URL = "url"
        const val TYPE = "slack"

        @JvmStatic
        @Throws(IOException::class)
        fun parse(xcp: XContentParser): Slack {
            var url: String? = null

            ensureExpectedToken(Token.START_OBJECT, xcp.currentToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()
                when (fieldName) {
                    URL -> url = xcp.text()
                    else -> {
                        error("Unexpected field: $fieldName, while parsing Slack destination")
                    }
                }
            }
            return Slack(requireNotNull(url) { "URL is null or empty" })
        }
    }

    // Complete JSON structure is now constructed in the notification plugin
    fun constructMessageContent(subject: String?, message: String): String {
        return if (Strings.isNullOrEmpty(subject)) message else "$subject \n\n $message"
    }
}
