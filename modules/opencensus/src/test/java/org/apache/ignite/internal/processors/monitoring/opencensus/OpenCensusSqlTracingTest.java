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

package org.apache.ignite.internal.processors.monitoring.opencensus;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.opencensus.trace.SpanId;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageRequest;
import org.apache.ignite.internal.processors.tracing.SpanType;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.apache.ignite.spi.tracing.TracingSpi;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.lang.Integer.parseInt;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA;
import static org.apache.ignite.internal.processors.tracing.SpanTags.CONSISTENT_ID;
import static org.apache.ignite.internal.processors.tracing.SpanTags.NAME;
import static org.apache.ignite.internal.processors.tracing.SpanTags.NODE;
import static org.apache.ignite.internal.processors.tracing.SpanTags.NODE_ID;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_CACHE_UPDATES;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_CACHE_UPDATE_FAILURES;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_IDX_RANGE_RESP_BYTES;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_IDX_RANGE_ROWS;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_PAGE_RESP_BYTES;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_PAGE_ROWS;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_QRY_TEXT;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_SCHEMA;
import static org.apache.ignite.internal.processors.tracing.SpanTags.tag;
import static org.apache.ignite.internal.processors.tracing.SpanType.COMMUNICATION_SOCKET_WRITE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_CACHE_UPDATE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_COMMAND_QRY_EXECUTE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_CURSOR_CANCEL;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_CURSOR_CLOSE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_CURSOR_OPEN;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_DML_QRY_EXEC_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_DML_QRY_RESP;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_DML_QRY_EXECUTE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_FAIL;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_IDX_RANGE_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_IDX_RANGE_RESP;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_ITER_CLOSE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_ITER_OPEN;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_NEXT_PAGE_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_PAGE_FETCH;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_PAGE_PREPARE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_PAGE_RESP;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_PAGE_WAIT;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_PARTITIONS_RESERVE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY_CANCEL_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY_EXECUTE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY_EXEC_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY_PARSE;
import static org.apache.ignite.spi.tracing.Scope.COMMUNICATION;
import static org.apache.ignite.spi.tracing.Scope.SQL;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;

/**
 * Tests tracing of SQL queries based on {@link OpenCensusTracingSpi}.
 */
public class OpenCensusSqlTracingTest extends AbstractTracingTest {
    /** Number of entries in all test caches. */
    private static final int CACHE_ENTRIES_CNT = 50;

    /** Page size for all queries. */
    private static final int PAGE_SIZE = 10;

    /** Key counter. */
    private final AtomicInteger keyCntr = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected TracingSpi getTracingSpi() {
        return new OpenCensusTracingSpi();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /** {@inheritDoc} */
    @Override public void before() throws Exception {
        super.before();

        grid(0).tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(SQL).build(),
            new TracingConfigurationParameters.Builder()
                .withSamplingRate(SAMPLING_RATE_ALWAYS).build());
    }

    /**
     * Tests tracing of local sql fields query.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLocalQuery() throws Exception {
        String schema = "ORG_SCHEMA";

        String table = createTableAndPopulate(Organization.class, REPLICATED, schema, 1);

        SpanId rootSpan = executeAndCheckRootSpan(
            new SqlFieldsQuery("SELECT addr FROM " + table + " WHERE id < ?")
                .setArgs(CACHE_ENTRIES_CNT)
                .setSchema(schema),
            ignite(GRID_CNT - 1));

        checkSpan(SQL_QRY_PARSE, rootSpan);
        checkSpan(SQL_CURSOR_OPEN, rootSpan);
        checkSpan(SQL_ITER_OPEN, rootSpan);

        SpanId iterSpan = checkSpan(SQL_ITER_OPEN, rootSpan);

        checkSpan(SQL_QRY_EXECUTE, iterSpan);

        int fetchedRows = findSpans(SQL_PAGE_FETCH, rootSpan).stream()
            .mapToInt(span -> integerAttribute(span, SQL_PAGE_ROWS))
            .sum();

        assertEquals(CACHE_ENTRIES_CNT, fetchedRows);

        checkSpan(SQL_ITER_CLOSE, rootSpan);
        checkSpan(SQL_CURSOR_CLOSE, rootSpan);
    }

    /**
     * Tests tracing of update query with skuipped reducer.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReducerSkippedUpdate() throws Exception {
        String table = createTableAndPopulate(Person.class, PARTITIONED, DFLT_SCHEMA, 1);

        SpanId rootSpan = executeAndCheckRootSpan(
            new SqlFieldsQueryEx("UPDATE " + table + " SET name=19229 WHERE id < ?", false)
                .setSkipReducerOnUpdate(true)
                .setArgs(CACHE_ENTRIES_CNT),
            startClientGrid(GRID_CNT));

        checkSpan(SQL_QRY_PARSE, rootSpan);

        SpanId dmlExecSpan = checkSpan(SQL_DML_QRY_EXECUTE, rootSpan);

        List<SpanId> execReqSpans = checkSpan(SQL_DML_QRY_EXEC_REQ, dmlExecSpan, GRID_CNT, null);

        int fetchedRows = 0;

        int cacheUpdates = 0;

        int cacheUpdateFailures = 0;

        for (int i = 0; i < GRID_CNT; i++) {
            SpanId execReqSpan = execReqSpans.get(i);

            checkSpan(SQL_PARTITIONS_RESERVE, execReqSpan);
            checkSpan(SQL_QRY_PARSE, execReqSpan, 2, null);

            SpanId iterSpan = checkSpan(SQL_ITER_OPEN, execReqSpan);

            checkSpan(SQL_QRY_EXECUTE, iterSpan);

            fetchedRows += findSpans(SQL_PAGE_FETCH, execReqSpan).stream()
                .mapToInt(span -> integerAttribute(span, SQL_PAGE_ROWS))
                .sum();

            List<SpanId> cacheUpdateSpans = findSpans(SQL_CACHE_UPDATE, execReqSpan);

            cacheUpdates += cacheUpdateSpans.stream()
                .mapToInt(span -> integerAttribute(span, SQL_CACHE_UPDATES))
                .sum();

            cacheUpdateFailures += cacheUpdateSpans.stream()
                .mapToInt(span -> integerAttribute(span, SQL_CACHE_UPDATE_FAILURES))
                .sum();

            checkSpan(SQL_ITER_CLOSE, execReqSpan);
            checkSpan(SQL_DML_QRY_RESP, execReqSpan);
        }

        assertEquals(CACHE_ENTRIES_CNT, fetchedRows);
        assertEquals(CACHE_ENTRIES_CNT, cacheUpdates);
        assertEquals(0, cacheUpdateFailures);
    }

    /**
     * Tests tracing of distributed join query which includes all communications between reducer and mapped nodes and
     * distributed lookups.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDistributedJoin() throws Exception {
        String prsnTable = createTableAndPopulate(Person.class, PARTITIONED, DFLT_SCHEMA, 1);

        String orgTable = createTableAndPopulate(Organization.class, PARTITIONED, DFLT_SCHEMA, 1);

        SpanId rootSpan = executeAndCheckRootSpan(new SqlFieldsQuery(
                "SELECT * FROM " + prsnTable + " AS p JOIN " + orgTable + " AS o ON o.id = p.id")
                .setDistributedJoins(true),
            startClientGrid(GRID_CNT));

        checkSpan(SQL_QRY_PARSE, rootSpan);
        checkSpan(SQL_CURSOR_OPEN, rootSpan);

        SpanId iterSpan = checkSpan(SQL_ITER_OPEN, rootSpan);

        List<SpanId> execReqSpans = checkSpan(SQL_QRY_EXEC_REQ, iterSpan, GRID_CNT, null);

        int idxRangeReqRows = 0;

        int preparedRows = 0;

        int fetchedRows = 0;

        for (int i = 0; i < GRID_CNT; i++) {
            SpanId execReqSpan = execReqSpans.get(i);

            checkSpan(SQL_PARTITIONS_RESERVE, execReqSpan);
            SpanId execSpan = checkSpan(SQL_QRY_EXECUTE, execReqSpan);

            List<SpanId> distrLookupReqSpans = findSpans(SQL_IDX_RANGE_REQ, execSpan);

            for (SpanId span : distrLookupReqSpans) {
                idxRangeReqRows += integerAttribute(span, SQL_IDX_RANGE_ROWS);

                checkSpan(SQL_IDX_RANGE_RESP, span);
            }

            preparedRows += integerAttribute(
                checkSpan(SQL_PAGE_PREPARE, execReqSpan), SQL_PAGE_ROWS);

            checkSpan(SQL_PAGE_RESP, execReqSpan);
        }

        SpanId pageFetchSpan = checkSpan(SQL_PAGE_FETCH, iterSpan);

        fetchedRows += integerAttribute(pageFetchSpan, SQL_PAGE_ROWS);

        checkSpan(SQL_PAGE_WAIT, pageFetchSpan);

        SpanId nexPageSpan = checkSpan(SQL_NEXT_PAGE_REQ, pageFetchSpan);

        preparedRows += integerAttribute(
            checkSpan(SQL_PAGE_PREPARE, nexPageSpan), SQL_PAGE_ROWS);

        checkSpan(SQL_PAGE_RESP, nexPageSpan);

        List<SpanId> pageFetchSpans = findSpans(SQL_PAGE_FETCH, rootSpan);

        for (SpanId span : pageFetchSpans) {
            fetchedRows += integerAttribute(span, SQL_PAGE_ROWS);

            checkSpan(SQL_PAGE_WAIT, span);

            List<SpanId> nextPageSpans = findSpans(SQL_NEXT_PAGE_REQ, span);

            if (!nextPageSpans.isEmpty()) {
                assertEquals(1, nextPageSpans.size());

                SpanId nextPageSpan = nextPageSpans.get(0);

                preparedRows += integerAttribute(
                    checkSpan(SQL_PAGE_PREPARE, nextPageSpan), SQL_PAGE_ROWS);

                checkSpan(SQL_PAGE_RESP, nextPageSpan);
            }
        }

        assertEquals(CACHE_ENTRIES_CNT, fetchedRows);
        assertEquals(CACHE_ENTRIES_CNT, preparedRows);
        assertEquals(CACHE_ENTRIES_CNT, idxRangeReqRows);

        checkSpan(SQL_QRY_CANCEL_REQ, rootSpan, GRID_CNT, null);

        SpanId cursorCloseSpan = checkSpan(SQL_CURSOR_CLOSE, rootSpan);

        checkSpan(SQL_ITER_CLOSE, cursorCloseSpan);
    }

    /**
     * Tests tracing of query request execution in
     *
     * @throws Exception If failed.
     */
    @Test
    public void testQueryExecutionWithParallelism() throws Exception {
        int qryParallelism = 2;

        String table = createTableAndPopulate(Person.class, PARTITIONED, DFLT_SCHEMA, qryParallelism);

        SpanId rootSpan = executeAndCheckRootSpan(
            new SqlFieldsQuery("SELECT * FROM " + table),
            startClientGrid(GRID_CNT));

        SpanId iterOpenSpan = checkSpan(SQL_ITER_OPEN, rootSpan);

        List<SpanId> qryExecspans = findSpans(SQL_QRY_EXEC_REQ, iterOpenSpan);

        assertEquals(GRID_CNT * qryParallelism, qryExecspans.size());
    }

    /**
     * Tests tracing of the SQL query next page request failure.
     *
     * @throws Exception If failed.
     */
    @Test
    @SuppressWarnings("Convert2MethodRef")
    public void testQueryRequestFailure() throws Exception {
        String table = createTableAndPopulate(Person.class, PARTITIONED, DFLT_SCHEMA, 1);

        IgniteEx cli = startClientGrid(GRID_CNT);

        try (
            FieldsQueryCursor<List<?>> cursor = cli.context().query().querySqlFields(
                new SqlFieldsQuery("SELECT * FROM " + table).setPageSize(PAGE_SIZE), false)
        ) {
            Iterator<List<?>> iter = cursor.iterator();

            spi(cli).blockMessages((node, msg) -> msg instanceof GridQueryNextPageRequest);

            IgniteInternalFuture<?> iterFut = GridTestUtils.runAsync(() -> iter.forEachRemaining(row -> {}));

            spi(cli).waitForBlocked(1);

            cli.context().query().runningQueries(-1).iterator().next().cancel();

            spi(cli).stopBlock();

            GridTestUtils.assertThrowsWithCause(() -> iterFut.get(), IgniteCheckedException.class);
        }

        handler().flush();

        SpanId rootSpan = checkSpan(SQL_QRY, null);

        SpanId cursorCancelSpan = checkSpan(SQL_CURSOR_CANCEL, rootSpan);

        SpanId cursorCloseSpan = checkSpan(SQL_CURSOR_CLOSE, cursorCancelSpan);

        SpanId iterCloseSpan = checkSpan(SQL_ITER_CLOSE, cursorCloseSpan);

        checkSpan(SQL_QRY_CANCEL_REQ, iterCloseSpan, GRID_CNT, null);

        List<SpanId> pageFetchSpans = findSpans(SQL_PAGE_FETCH, rootSpan);

        int nextPageReqs = 0;

        for (SpanId span : pageFetchSpans) {
            List<SpanId> nextPageReqSpans = findSpans(SQL_NEXT_PAGE_REQ, span);

            if (!nextPageReqSpans.isEmpty()) {
                assertEquals(1, nextPageReqSpans.size());

                ++nextPageReqs;

                checkSpan(SQL_FAIL, nextPageReqSpans.get(0));
            }
        }

        assertTrue(nextPageReqs > 0);
    }

    /**
     * Tests attributes that show the size of messages that were transferred between nodes during SQL execution.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMessageSizeAttributes() throws Exception {
        String prsnTable = createTableAndPopulate(Person.class, PARTITIONED, DFLT_SCHEMA, 1);

        String orgTable = createTableAndPopulate(Organization.class, PARTITIONED, DFLT_SCHEMA, 1);

        IgniteEx cli = startClientGrid(GRID_CNT);

        cli.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(SQL).build(),
            new TracingConfigurationParameters.Builder()
                .withIncludedScopes(ImmutableSet.of(COMMUNICATION))
                .withSamplingRate(SAMPLING_RATE_ALWAYS).build());

        executeAndCheckRootSpan(new SqlFieldsQuery(
                "SELECT * FROM " + prsnTable + " AS p JOIN " + orgTable + " AS o ON o.id = p.id")
                .setDistributedJoins(true), cli);

        checkChildSocketWriteSpanAttribute(SQL_IDX_RANGE_REQ, SQL_IDX_RANGE_RESP_BYTES);

        checkChildSocketWriteSpanAttribute(SQL_NEXT_PAGE_REQ, SQL_PAGE_RESP_BYTES);
    }

    /**
     * Tests tracing of the SQL command execution.
     */
    @Test
    public void testCommandExecution() throws Exception {
        SpanId rootSpan = executeAndCheckRootSpan(
            new SqlFieldsQuery("CREATE TABLE test_table(id INT PRIMARY KEY, val VARCHAR)"),
            startClientGrid(GRID_CNT));

        checkSpan(SQL_QRY_PARSE, rootSpan);
        checkSpan(SQL_COMMAND_QRY_EXECUTE, rootSpan);
    }

    /**
     * Checks that a span of the specified type exists and has a socket write child span with specified attribute.
     *
     * @param type Type of the span.
     * @param attr Attribute to check.
     */
    private void checkChildSocketWriteSpanAttribute(SpanType type, String attr) {
        List<SpanId> spans = findSpans(type, null);

        assertFalse(spans.isEmpty());

        for (SpanId span : spans) {
            List<SpanId> sockWriteSpans = findSpans(COMMUNICATION_SOCKET_WRITE, span);

            if (!sockWriteSpans.isEmpty()) {
                assertEquals(1, sockWriteSpans.size());

                assertTrue(integerAttribute(sockWriteSpans.get(0), attr) > 0);
            }
        }
    }

    /**
     * Checks whether parent span has a single child span with specified type.
     *
     * @param type Span type.
     * @param parentSpan Parent span id.
     * @return Id of the the child span.
     */
    private SpanId checkSpan(SpanType type, SpanId parentSpan) {
        return checkSpan(type, parentSpan,1, null).get(0);
    }

    /**
     * Finds child spans with specified type and parent span.
     *
     * @param type Span type.
     * @param parentSpanId Parent span id.
     * @return Ids of the found spans.
     */
    private List<SpanId> findSpans(SpanType type, SpanId parentSpanId) {
        return handler().allSpans()
            .filter(span -> parentSpanId != null ?
                parentSpanId.equals(span.getParentSpanId()) && type.spanName().equals(span.getName()) :
                type.spanName().equals(span.getName()))
            .map(span -> span.getContext().getSpanId())
            .collect(Collectors.toList());
    }

    /**
     * Obtains integer value of the attribtute from span with specified id.
     *
     * @param spanId Id of the target span.
     * @param tag Tag of the attribute.
     * @return Value of the attribute.
     */
    private int integerAttribute(SpanId spanId, String tag) {
        return parseInt(attributeValueToString(handler()
            .spanById(spanId)
            .getAttributes()
            .getAttributeMap()
            .get(tag)));
    }

    /**
     * Executes the query and checks the root span of query execution.
     *
     * @param qry Queyr to execute.
     * @param ignite Node which will be used for execution.
     * @return Id of thre root span.
     */
    private SpanId executeAndCheckRootSpan(SqlFieldsQuery qry, IgniteEx ignite) throws Exception {
        ignite.context().query().querySqlFields(qry.setPageSize(PAGE_SIZE), false).getAll();

        handler().flush();

        return checkSpan(
            SQL_QRY,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put(NODE_ID, ignite.localNode().id().toString())
                .put(tag(NODE, CONSISTENT_ID), ignite.localNode().consistentId().toString())
                .put(tag(NODE, NAME), ignite.name())
                .put(SQL_QRY_TEXT, qry.getSql())
                .put(SQL_SCHEMA, qry.getSchema() == null ? DFLT_SCHEMA : qry.getSchema())
                .build()
        ).get(0);
    }

    /**
     * @return Name of the table which was created.
     */
    private String createTableAndPopulate(Class<?> cls, CacheMode mode, String schema, int qryParallelism) {
        IgniteCache<Integer, Object> cache = grid(0).createCache(
            new CacheConfiguration<Integer, Object>(cls.getSimpleName() + mode)
                .setIndexedTypes(Integer.class, cls)
                .setCacheMode(mode)
                .setQueryParallelism(qryParallelism)
                .setSqlSchema(schema)
        );

        for (int i = 0; i < CACHE_ENTRIES_CNT; i++)
            cache.put(keyCntr.getAndIncrement(), cls == Organization.class ? new Organization(i, i) : new Person(i, i));

        return schema + '.' + cls.getSimpleName();
    }

    /** */
    public static class Person {
        /** */
        @QuerySqlField(index = true)
        private int id;

        /** */
        @QuerySqlField
        private int name;

        /** */
        public Person(int id, int name) {
            this.id = id;
            this.name = name;
        }
    }

    /** */
    public static class Organization {
        /** */
        @QuerySqlField(index = true)
        private int id;

        /** */
        @QuerySqlField
        private int addr;

        /** */
        public Organization(int id, int addr) {
            this.id = id;
            this.addr = addr;
        }
    }
}
