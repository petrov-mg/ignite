/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.ignite;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.logger.log4j.Log4JLogger;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import static org.apache.ignite.SqlTracingBenchmarks.BenchmarkContext.SELECT_QRY_PAGE_SIZE;
import static org.apache.ignite.SqlTracingBenchmarks.BenchmarkContext.SELECT_RANGE;
import static org.apache.ignite.SqlTracingBenchmarks.BenchmarkContext.TABLE_POPULATION;
import static org.apache.ignite.SqlTracingBenchmarks.BenchmarkContext.UPDATE_RANGE;
import static org.apache.ignite.SqlTracingBenchmarks.BenchmarkContext.UPDATE_QRY_PAGE_SIZE;
import static org.apache.ignite.spi.tracing.Scope.SQL;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;

/** */
public class SqlTracingBenchmarks {
    /** */
    @State(Scope.Benchmark)
    public static class BenchmarkContext {
        /** */
        public static final int NODES_CNT = 2;

        /** */
        public static final int TABLE_POPULATION = 20000;

        /** */
        public static final int SELECT_RANGE = 1;

        /** */
        public static final int UPDATE_RANGE = 1;

        /** */
        public static final int SELECT_QRY_PAGE_SIZE = 1024;

        /** */
        public static final int UPDATE_QRY_PAGE_SIZE = 1024;

        /** */
        private IgniteEx cli;

        /** */
        @Setup
        public void setUp() throws Exception {
            for (int i = 0; i < NODES_CNT; i++)
                startGrid(i, false);

            cli = startGrid(NODES_CNT, true);

            GridQueryProcessor qryProc = cli.context().query();

            qryProc.querySqlFields(
               new SqlFieldsQuery("CREATE TABLE test_table (id LONG PRIMARY KEY, val LONG)"), false);

            qryProc.querySqlFields(new SqlFieldsQuery("CREATE INDEX val_idx ON test_table (val)"), false);

            for (long key = 1; key <= TABLE_POPULATION; ++key) {
                qryProc.querySqlFields(
                    new SqlFieldsQuery("INSERT INTO test_table (id, val) VALUES (?, ?)").setArgs(key, key),
                    true
                );
            }

            cli.tracingConfiguration().set(
                new TracingConfigurationCoordinates.Builder(SQL).build(),
                new TracingConfigurationParameters.Builder()
                    .withSamplingRate(SAMPLING_RATE_ALWAYS).build());
        }

        /** */
        @TearDown
        public void tearDown() {
            Ignition.stopAll(true);
        }

        /** */
        private IgniteEx startGrid(int idx, boolean clientMode) throws Exception {
            return (IgniteEx) Ignition.start(new IgniteConfiguration()
                .setIgniteInstanceName("node-" + idx)
                .setGridLogger(new Log4JLogger("modules/core/src/test/config/log4j-test.xml"))
                .setTracingSpi(new OpenCensusTracingSpi())
                .setClientMode(clientMode)
                .setConsistentId(Integer.toString(idx)));
        }

        /** */
        public IgniteEx clientNode() {
            return cli;
        }
    }

    /** */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Warmup(iterations = 1, time = 10)
    @Measurement(iterations = 10, time = 10)
    @Fork(value = 1)
    public void benchmarkSelect(BenchmarkContext ctx) {
        long lowId = ThreadLocalRandom.current().nextLong(TABLE_POPULATION - SELECT_RANGE);

        SqlFieldsQuery qry = SELECT_RANGE > 1 ?
            new SqlFieldsQuery("SELECT id, val FROM test_table WHERE id BETWEEN ? and ?")
                .setArgs(lowId, lowId + SELECT_RANGE)
                .setPageSize(SELECT_QRY_PAGE_SIZE):
            new SqlFieldsQuery("SELECT id, val FROM test_table WHERE id = ?")
                .setArgs(lowId);


        try (
            FieldsQueryCursor<List<?>> cursor = ctx.clientNode().context().query().querySqlFields(qry, false)
        ) {
            cursor.iterator().forEachRemaining(val -> {});
        }
    }

    /** */
   /* @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Warmup(iterations = 1, time = 10)
    @Measurement(iterations = 10, time = 10)
    @Fork(value = 1)
    public void benchmarkUpdate(BenchmarkContext ctx) {
        long lowId = ThreadLocalRandom.current().nextLong(TABLE_POPULATION - UPDATE_RANGE);

        SqlFieldsQuery qry = UPDATE_RANGE > 1 ?
            new SqlFieldsQuery("UPDATE test_table SET val = (val + 1) WHERE id BETWEEN ? AND ?")
                .setArgs(lowId, lowId + UPDATE_RANGE)
                .setPageSize(UPDATE_QRY_PAGE_SIZE) :
            new SqlFieldsQuery("UPDATE test_table SET val = (val + 1) WHERE id = ?")
                .setArgs(lowId);

        try (
            FieldsQueryCursor<List<?>> ignored = ctx.clientNode().context().query().querySqlFields(qry, false)
        ) {
            // No-op.
        }
    }*/
}
