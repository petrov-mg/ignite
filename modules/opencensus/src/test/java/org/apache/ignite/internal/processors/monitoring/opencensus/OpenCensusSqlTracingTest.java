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
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
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
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA;
import static org.apache.ignite.internal.processors.tracing.SpanTags.CONSISTENT_ID;
import static org.apache.ignite.internal.processors.tracing.SpanTags.NAME;
import static org.apache.ignite.internal.processors.tracing.SpanTags.NODE;
import static org.apache.ignite.internal.processors.tracing.SpanTags.NODE_ID;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_DISTR_LOOKUP_RESULT_BYTES;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_DISTR_LOOKUP_RESULT_ROWS;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_MAP_RESULT_PAGE_BYTES;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_QRY_SCHEMA;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_QRY_TEXT;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_RESULT_PAGE_ROWS;
import static org.apache.ignite.internal.processors.tracing.SpanTags.tag;
import static org.apache.ignite.internal.processors.tracing.SpanType.COMMUNICATION_SOCKET_WRITE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_CURSOR_CLOSE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_ITER_OPEN;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_IDX_RANGE_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_IDX_RANGE_RESP;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_FAIL;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_NEXT_PAGE_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY_CANCEL;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY_EXEC_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_PAGE_RESP;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_PAGE_WAIT;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY_UPDATE_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY_UPDATE_RESP;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_CURSOR_OPEN;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY_EXECUTE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY_PARSE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_PAGE_PREPARE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_NEXT_PAGE_FETCH;
import static org.apache.ignite.spi.tracing.Scope.*;
import static org.apache.ignite.spi.tracing.Scope.SQL;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;

/**
 * Tests tracing of SQL queries based on {@link OpenCensusTracingSpi}.
 */
public class OpenCensusSqlTracingTest extends AbstractTracingTest {
    /** Test schema for persons cache. */
    private static final String PERSON_SCHEMA = "person_schema";

    /** Test table on the partitioned underlying cache for update and distributed join queries testing. */
    private static final String PERSON_TABLE = PERSON_SCHEMA + '.' + Person.class.getSimpleName();

    /** Test schema for partitioned organization cache. */
    private static final String PARTITIONED_ORG_SCHEMA = "partitioned_org_schema";

    /** Test table on the partitioned underlying cache for distributed join queries testing. */
    private static final String PARTITIONED_ORG_TABLE = PARTITIONED_ORG_SCHEMA + '.' + Organization.class.getSimpleName();

    /** Test schema for replicated organization cache. */
    private static final String REPLICATED_ORG_SCHEMA = "replicated_org_schema";

    /** Test table on the replicated underlying cache for local query testing. */
    private static final String REPLICATED_ORG_TABLE = REPLICATED_ORG_SCHEMA + '.' + Organization.class.getSimpleName();

    /** Number of entries in all test caches. */
    private static final int CACHE_ENTRIES_CNT = 100;

    /** Page size for all queries. */
    private static final int PAGE_SIZE = 20;

    /** {@inheritDoc} */
    @Override protected TracingSpi<?> getTracingSpi() {
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

        IgniteEx srv = grid(0);

        srv.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(SQL).build(),
            new TracingConfigurationParameters.Builder()
                .withSamplingRate(SAMPLING_RATE_ALWAYS).build());

        IgniteCache<Integer, Person> prsnCache = srv.createCache(
            new CacheConfiguration<Integer, Person>("person-cache")
                .setIndexedTypes(Integer.class, Person.class)
                .setSqlSchema(PERSON_SCHEMA)
        );

        IgniteCache<Integer, Organization> replicatedOrgCache = srv.createCache(
            new CacheConfiguration<Integer, Organization>("replicated-org-cache")
                .setIndexedTypes(Integer.class, Organization.class)
                .setCacheMode(REPLICATED)
                .setSqlSchema(REPLICATED_ORG_SCHEMA)
        );

        IgniteCache<Integer, Organization> partitionedOrgCache = srv.createCache(
            new CacheConfiguration<Integer, Organization>("partitioned-org-cache")
                .setIndexedTypes(Integer.class, Organization.class)
                .setSqlSchema(PARTITIONED_ORG_SCHEMA)
        );

        int keyCntr = 0;

        for (int i = 0; i < CACHE_ENTRIES_CNT; i++) {
            prsnCache.put(keyCntr++, new Person(i, i));

            replicatedOrgCache.put(keyCntr++, new Organization(i, i));

            partitionedOrgCache.put(keyCntr++, new Organization(i, i));
        }
    }

    /**
     * Tests tracing of local sql fields query.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLocalQuery() throws Exception {
        SpanId rootSpan = executeAndCheckRootSpan(
            new SqlFieldsQuery("SELECT addr FROM " + REPLICATED_ORG_TABLE + " WHERE id < ?")
                .setArgs(CACHE_ENTRIES_CNT),
            ignite(GRID_CNT - 1));

        SpanId cursorSpan = checkChildSpan(SQL_CURSOR_OPEN, rootSpan);

        checkChildSpan(SQL_QRY_PARSE, cursorSpan);
        checkChildSpan(SQL_ITER_OPEN, rootSpan);

        SpanId iterSpan = checkChildSpan(SQL_ITER_OPEN, rootSpan);

        checkChildSpan(SQL_QRY_EXECUTE, iterSpan);

        int fetchedRows = findSpans(SQL_NEXT_PAGE_FETCH, rootSpan).stream()
            .mapToInt(span -> integerAttribute(span, SQL_RESULT_PAGE_ROWS))
            .sum();

        assertEquals(CACHE_ENTRIES_CNT, fetchedRows);

        checkChildSpan(SQL_CURSOR_CLOSE, rootSpan);
    }

    /**
     * Tests tracing of update query with skuipped reducer.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReducerSkippedUpdate() throws Exception {
        SpanId rootSpan = executeAndCheckRootSpan(
            new SqlFieldsQueryEx("UPDATE " + PERSON_TABLE + " SET name=19229 WHERE id < ?;", false)
                .setSkipReducerOnUpdate(true)
                .setArgs(CACHE_ENTRIES_CNT),
            startClientGrid(GRID_CNT));

        SpanId cursorSpan = checkChildSpan(SQL_CURSOR_OPEN, rootSpan);

        checkChildSpan(SQL_QRY_PARSE, cursorSpan);

        List<SpanId> mapReqSpans = checkChildSpans(SQL_QRY_UPDATE_REQ, GRID_CNT, cursorSpan);

        int fetchedRows = 0;

        for (int i = 0; i < GRID_CNT; i++) {
            SpanId mapReqSpan = mapReqSpans.get(i);

            checkChildSpans(SQL_QRY_PARSE, 2, mapReqSpan);

            checkChildSpan(SQL_QRY_EXECUTE, mapReqSpan);

            fetchedRows += findSpans(SQL_NEXT_PAGE_FETCH, mapReqSpan).stream()
                .mapToInt(span -> integerAttribute(span, SQL_RESULT_PAGE_ROWS))
                .sum();

            checkChildSpan(SQL_QRY_UPDATE_RESP, mapReqSpans.get(i));
        }

        assertEquals(CACHE_ENTRIES_CNT, fetchedRows);
    }

    /**
     * Tests tracing of distributed join query which includes all communications between reducer and mapped nodes and
     * distributed lookups.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDistributedJoin() throws Exception {
        SpanId rootSpan = executeAndCheckRootSpan(new SqlFieldsQuery(
                "SELECT * FROM " + PERSON_TABLE + " AS p JOIN " + PARTITIONED_ORG_TABLE + " AS o ON o.id = p.id")
                .setDistributedJoins(true),
            startClientGrid(GRID_CNT));

        SpanId cursorSpan = checkChildSpan(SQL_CURSOR_OPEN, rootSpan);

        checkChildSpan(SQL_QRY_PARSE, cursorSpan);

        SpanId iterSpan = checkChildSpan(SQL_ITER_OPEN, rootSpan);

        List<SpanId> mapReqSpans = checkChildSpans(SQL_QRY_EXEC_REQ, GRID_CNT, iterSpan);

        int distrLookupRows = 0;

        int preparedRows = 0;

        int fetchedRows = 0;

        for (int i = 0; i < GRID_CNT; i++) {
            SpanId mapReqSpan = mapReqSpans.get(i);

            SpanId execSpan = checkChildSpan(SQL_QRY_EXECUTE, mapReqSpan);

            List<SpanId> distrLookupReqSpans = findSpans(SQL_IDX_RANGE_REQ, execSpan);

            for (SpanId span : distrLookupReqSpans) {
                distrLookupRows += integerAttribute(span, SQL_DISTR_LOOKUP_RESULT_ROWS);

                checkChildSpan(SQL_IDX_RANGE_RESP, span);
            }

            preparedRows += integerAttribute(
                checkChildSpan(SQL_PAGE_PREPARE, mapReqSpan), SQL_RESULT_PAGE_ROWS);

            checkChildSpan(SQL_PAGE_RESP, mapReqSpan);
        }

        SpanId pageFetchSpan = checkChildSpan(SQL_NEXT_PAGE_FETCH, iterSpan);

        fetchedRows += integerAttribute(pageFetchSpan, SQL_RESULT_PAGE_ROWS);

        checkChildSpan(SQL_PAGE_WAIT, pageFetchSpan);

        SpanId nexPageSpan = checkChildSpan(SQL_NEXT_PAGE_REQ, pageFetchSpan);

        preparedRows += integerAttribute(
            checkChildSpan(SQL_PAGE_PREPARE, nexPageSpan), SQL_RESULT_PAGE_ROWS);

        checkChildSpan(SQL_PAGE_RESP, nexPageSpan);

        List<SpanId> pageFetchSpans = findSpans(SQL_NEXT_PAGE_FETCH, rootSpan);

        for (SpanId span : pageFetchSpans) {
            fetchedRows += integerAttribute(span, SQL_RESULT_PAGE_ROWS);

            checkChildSpan(SQL_PAGE_WAIT, span);

            List<SpanId> nextPageSpans = findSpans(SQL_NEXT_PAGE_REQ, span);

            if (!nextPageSpans.isEmpty()) {
                assertEquals(1, nextPageSpans.size());

                SpanId nextPageSpan = nextPageSpans.get(0);

                preparedRows += integerAttribute(
                    checkChildSpan(SQL_PAGE_PREPARE, nextPageSpan), SQL_RESULT_PAGE_ROWS);

                checkChildSpan(SQL_PAGE_RESP, nextPageSpan);
            }
        }

        assertEquals(CACHE_ENTRIES_CNT, fetchedRows);
        assertEquals(CACHE_ENTRIES_CNT, preparedRows);
        assertEquals(CACHE_ENTRIES_CNT, distrLookupRows);

        checkChildSpan(SQL_CURSOR_CLOSE, rootSpan);
        checkChildSpans(SQL_QRY_CANCEL, GRID_CNT, rootSpan);
    }

    /**
     * Tests tracing of mapped node failure.
     *
     * @throws Exception If failed.
     */
    @Test
    @SuppressWarnings("Convert2MethodRef")
    public void testMappedNodeFailure() throws Exception {
        IgniteEx cli = startClientGrid(GRID_CNT);

        try (
            FieldsQueryCursor<List<?>> cursor = cli.cache(DEFAULT_CACHE_NAME).query(
                new SqlFieldsQuery("SELECT * FROM " + PERSON_TABLE).setPageSize(PAGE_SIZE))
        ) {
            Iterator<List<?>> iter = cursor.iterator();

            spi(cli).blockMessages((nod, msg) -> msg instanceof GridQueryNextPageRequest);

            IgniteInternalFuture<?> iterFut = GridTestUtils.runAsync(() -> iter.forEachRemaining(row -> {}));

            spi(cli).waitForBlocked(1);

            cli.context().query().runningQueries(-1).iterator().next().cancel();

            spi(cli).stopBlock();

            GridTestUtils.assertThrowsWithCause(() -> iterFut.get(), IgniteCheckedException.class);
        }

        handler().flush();

        SpanId rootSpan = checkChildSpan(SQL_QRY, null);

        List<SpanId> pageFetchSpans = findSpans(SQL_NEXT_PAGE_FETCH, rootSpan);

        int nextPageReqs = 0;

        for (SpanId span : pageFetchSpans) {
            List<SpanId> nextPageReqSpans = findSpans(SQL_NEXT_PAGE_REQ, span);

            if (!nextPageReqSpans.isEmpty()) {
                assertEquals(1, nextPageReqSpans.size());

                ++nextPageReqs;

                checkChildSpan(SQL_FAIL, nextPageReqSpans.get(0));
            }
        }

        assertTrue(nextPageReqs > 0);
    }

    /**
     * Tests attributes that show the size of data that was transferred between nodes during SQL execution.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTransferredDataSizeAttribute() throws Exception {
        IgniteEx cli = startClientGrid(GRID_CNT);

        cli.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(SQL).build(),
            new TracingConfigurationParameters.Builder()
                .withIncludedScopes(ImmutableSet.of(COMMUNICATION))
                .withSamplingRate(SAMPLING_RATE_ALWAYS).build());

        executeAndCheckRootSpan(new SqlFieldsQuery(
                "SELECT * FROM " + PERSON_TABLE + " AS p JOIN " + PARTITIONED_ORG_TABLE + " AS o ON o.id = p.id")
                .setDistributedJoins(true), cli);

        checkResponseSocketWriteAttribute(SQL_IDX_RANGE_REQ, SQL_DISTR_LOOKUP_RESULT_BYTES);

        checkResponseSocketWriteAttribute(SQL_NEXT_PAGE_REQ, SQL_MAP_RESULT_PAGE_BYTES);
    }

    /**
     * Checks that a span of the specified type exists and has a communication socket write child span with
     * specified attribute.
     *
     * @param type Type of the span.
     * @param attr Attribute to check.
     */
    private void checkResponseSocketWriteAttribute(SpanType type, String attr) {
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
    private SpanId checkChildSpan(SpanType type, SpanId parentSpan) {
        return checkSpan(type, parentSpan,1, null).get(0);
    }

    /**
     * Checks whether parent span has a specified count of child spans of specified type.
     *
     * @param type Span type.
     * @param parentSpan Parent span id.
     * @param cnt Expected spans count.
     * @return Id of child spans.
     */
    private List<SpanId> checkChildSpans(SpanType type, int cnt, SpanId parentSpan) {
       return checkSpan(type, parentSpan, cnt, null);
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
    private SpanId executeAndCheckRootSpan(SqlFieldsQuery qry, IgniteEx ignite) throws IgniteInterruptedCheckedException {
        ignite.cache(DEFAULT_CACHE_NAME).query(qry.setPageSize(PAGE_SIZE)).getAll();

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
                .put(SQL_QRY_SCHEMA, DFLT_SCHEMA)
                .build()
        ).get(0);
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
