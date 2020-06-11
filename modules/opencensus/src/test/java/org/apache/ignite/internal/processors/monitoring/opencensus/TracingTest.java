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

import io.opencensus.exporter.trace.zipkin.ZipkinExporterConfiguration;
import io.opencensus.exporter.trace.zipkin.ZipkinTraceExporter;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.tracing.CounterLoggingSpan;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.Tracing;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.apache.ignite.spi.tracing.TracingSpi;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.spi.tracing.Scope.COMMUNICATION;
import static org.apache.ignite.spi.tracing.Scope.DISCOVERY;
import static org.apache.ignite.spi.tracing.Scope.EXCHANGE;
import static org.apache.ignite.spi.tracing.Scope.SQL;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;

public class TracingTest extends AbstractTracingTest {
    @BeforeClass
    public static void beforeTests() {
        /*JaegerTraceExporter.createAndRegister(JaegerExporterConfiguration.builder()
            .setThriftEndpoint("http://127.0.0.1:14268/api/traces")
            .setServiceName("ignite")
            .build());*/
        ZipkinTraceExporter.createAndRegister(ZipkinExporterConfiguration.builder()
            .setV2Url("http://localhost:9411/api/v2/spans")
            .setServiceName("ignite")
            .build());
    }

    @Override protected TracingSpi getTracingSpi() {
        return new OpenCensusTracingSpi();
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setTracingSpi(new OpenCensusTracingSpi())
            .setTransactionConfiguration(new TransactionConfiguration());
    }

    private static final String TEST_SCHEMA_FIRST = "test_schema_first";

    private static final String TEST_SCHEMA_SECOND = "test_schema_second";

    private static final String FIRST_TABLE = TEST_SCHEMA_FIRST + '.' + FirstValue.class.getSimpleName();

    private static final String SECOND_TABLE = TEST_SCHEMA_SECOND + '.' + SecondValue.class.getSimpleName();

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        handler().flush();
    }

    /** */
    @Test
    public void sql() throws Exception {
        IgniteEx srv = ignite(0);

        srv.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(DISCOVERY).build(),
            new TracingConfigurationParameters.Builder()
                .withSamplingRate(SAMPLING_RATE_ALWAYS)
                .withIncludedScopes(Stream.of(SQL, EXCHANGE, DISCOVERY, COMMUNICATION).collect(Collectors.toSet()))
                .build());

        srv.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(EXCHANGE).build(),
            new TracingConfigurationParameters.Builder()
                .withSamplingRate(SAMPLING_RATE_ALWAYS)
                .withIncludedScopes(Stream.of(SQL, EXCHANGE, DISCOVERY, COMMUNICATION).collect(Collectors.toSet()))
                .build());

        srv.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(COMMUNICATION).build(),
            new TracingConfigurationParameters.Builder()
                .withSamplingRate(SAMPLING_RATE_ALWAYS)
                .withIncludedScopes(Stream.of(SQL, EXCHANGE, DISCOVERY, COMMUNICATION).collect(Collectors.toSet()))
                .build());

        srv.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(SQL).build(),
            new TracingConfigurationParameters.Builder()
                .withSamplingRate(SAMPLING_RATE_ALWAYS)
                .withIncludedScopes(Stream.of(SQL, EXCHANGE, DISCOVERY, COMMUNICATION).collect(Collectors.toSet()))
                .build());

        IgniteCache<Integer, FirstValue> first = srv.createCache(
            new CacheConfiguration<Integer, FirstValue>("test-cache-first")
                .setIndexedTypes(Integer.class, FirstValue.class)
                .setCacheMode(PARTITIONED)
                .setSqlSchema(TEST_SCHEMA_FIRST)
        );

        int key1 = keyForNode(1, "test-cache-first");
        int key2 = keyForNode(1, "test-cache-first");
        int key3 = keyForNode(1, "test-cache-first");
        int key4 = keyForNode(1, "test-cache-first");

        first.put(key1, new FirstValue(0, 30));
        first.put(key2, new FirstValue(1, 40));
        first.put(key3, new FirstValue(2, 10));
        first.put(key4, new FirstValue(3, 20));


//       IgniteCache<Integer, SecondValue> second = srv.createCache(
//            new CacheConfiguration<Integer, SecondValue>("test-cache-second")
//                .setIndexedTypes(Integer.class, SecondValue.class)
//                .setCacheMode(PARTITIONED)
//                .setSqlSchema(TEST_SCHEMA_SECOND)
//        );
//
//        keyForNode(2, "test-cache-second");
//        keyForNode(2, "test-cache-second");
//        keyForNode(2, "test-cache-second");
//        keyForNode(2, "test-cache-second");
//
//        second.put(key1, new SecondValue(1, 30));
//        second.put(key2, new SecondValue(1, 40));
//        second.put(key3, new SecondValue(1, 10));
//        second.put(key4, new SecondValue(0, 20));

        FieldsQueryCursor<List<?>> cursor = first.query(new SqlFieldsQuery(
            "UPDATE " + FIRST_TABLE + " SET first=666 WHERE id=0 OR id=1;"
            )/*.setPartitions(
            grid(0).affinity("test-cache-first").partition(key1),
            grid(0).affinity("test-cache-first").partition(key2),
            grid(0).affinity("test-cache-first").partition(key3),
            grid(0).affinity("test-cache-first").partition(key4))*/
        );


        List<List<?>> res = cursor.getAll();

        System.out.println(res);

        handler().flush();

//        first.query(new SqlFieldsQuery(
//            "UPDATE " + FIRST_TABLE +
//                " SET first = (SELECT st.second FROM " + SECOND_TABLE + " AS st WHERE id=0)" +
//                " WHERE id=0;"
//        ).setDistributedJoins(true)).getAll();
//
//        List<List<?>> res = first.query(new SqlFieldsQuery(
//            "SELECT * FROM " + FIRST_TABLE
//        )).getAll();
//
//        System.err.println(res);
//
//        res = first.query(new SqlFieldsQuery(
//            "SELECT * FROM " + SECOND_TABLE
//        ).setDistributedJoins(true)).getAll();
//
//        System.err.println(res);

    }

    /** */
    private static final AtomicInteger KEY_CNT = new AtomicInteger();

    /** */
    private int keyForNode(int nodeIdx, String cacheName) {
        int res;

        int keyIdx = 0;

        AtomicInteger cnt = new AtomicInteger(0);

        do {
            res = keyForNode(
                grid(0).affinity(cacheName),
                cnt,
                grid(nodeIdx).localNode());
        }
        while (keyIdx++ != KEY_CNT.get());

        KEY_CNT.incrementAndGet();

        return res;
    }

    /** */
    public static class FirstValue {
        /** */
        @QuerySqlField(index = true)
        private int id;

        /** */
        @QuerySqlField
        private int first;

        /** */
        public FirstValue(int id, int first) {
            this.id = id;
            this.first = first;
        }

        /** */
        public int getId() {
            return id;
        }

        /** */
        public void setId(int id) {
            this.id = id;
        }

        /** */
        public int getFirst() {
            return first;
        }

        /** */
        public void setFirst(int first) {
            this.first = first;
        }
    }

    /** */
    public static class SecondValue {
        /** */
        @QuerySqlField(index = true)
        private int id;

        /** */
        @QuerySqlField
        private int second;

        /** */
        public int getId() {
            return id;
        }

        /** */
        public void setId(int id) {
            this.id = id;
        }

        /** */
        public SecondValue(int id, int second) {
            this.id = id;
            this.second = second;
        }

        /** */
        public int getSecond() {
            return second;
        }

        /** */
        public void setSecond(int second) {
            this.second = second;
        }
    }

    /**
     *
     */
    private void increment(String counter) {
        MTC.<CounterLoggingSpan>span().incrementCounter(counter);

    }

    /** */
    Tracing tracing(IgniteEx ignte) {
        return ignte.context().tracing();
    }
}