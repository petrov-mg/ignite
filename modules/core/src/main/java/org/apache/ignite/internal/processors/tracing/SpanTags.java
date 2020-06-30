/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.tracing;

/**
 * List of tags that can be used to decorate spans.
 */
public class SpanTags {
    /** Tag parts default delimiter. */
    private static final String TAG_PARTS_DELIMITER = ".";

    /**
     * List of basic tags. Can be combined together to get more composite tags.
     * Don't forget to add new tags here and use constant reference instead of raw string creation.
     * Frequently used composite tags can be also declared here.
     */

    /** */
    public static final String NODE = "node";

    /** */
    public static final String ID = "id";

    /** */
    public static final String ORDER = "order";

    /** */
    public static final String EVENT = "event";

    /** */
    public static final String NAME = "name";

    /** */
    public static final String TYPE = "type";

    /** */
    public static final String INITIAL = "initial";

    /** */
    public static final String RESULT = "result";

    /** */
    public static final String ERROR = "error";

    /** */
    public static final String EXCHANGE = "exchange";

    /** */
    public static final String CONSISTENT_ID = "consistent.id";

    /** */
    public static final String TOPOLOGY_VERSION = "topology.version";

    /** */
    public static final String MAJOR = "major";

    /** */
    public static final String MINOR = "minor";

    /** */
    public static final String EVENT_NODE = tag(EVENT, NODE);

    /** */
    public static final String NODE_ID = tag(NODE, ID);

    /** */
    public static final String MESSAGE = "message";

    /** */
    public static final String MESSAGE_CLASS = "message.class";

    /** Text of the SQL query or queries in case of a map request. */
    public static final String SQL_QRY_TEXT = "sql.query.text";

    /** Schema of the sql query. */
    public static final String SQL_QRY_SCHEMA = "sql.query.schema";

    /** Number of rows that current result page contains. */
    public static final String SQL_RESULT_PAGE_ROWS = "sql.result.page.rows";

    /**
     * Result page response from the mapped node in bytes.
     * Note that this tag will be attached to {@link SpanType#COMMUNICATION_SOCKET_WRITE} span that represents sending
     * of a result page response message.
     */
    public static final String SQL_MAP_RESULT_PAGE_BYTES = "sql.map.result.page.bytes";

    /** Number of rows that was obtained by distributed lookup. */
    public static final String SQL_DISTR_LOOKUP_RESULT_ROWS = "sql.distributed.lookup.result.rows";

    /**
     * Distributed lookup responce in bytes.
     * Note that this tag will be attached to {@link SpanType#COMMUNICATION_SOCKET_WRITE} span that represents sending
     * of a distributed lookup response message.
     */
    public static final String SQL_DISTR_LOOKUP_RESULT_BYTES = "sql.distributed.lookup.result.bytes";

    /** */
    private SpanTags() {}

    /**
     * @param tagParts String parts of composite tag.
     * @return Composite tag with given parts joined using delimiter.
     */
    public static String tag(String... tagParts) {
        return String.join(TAG_PARTS_DELIMITER, tagParts);
    }
}
