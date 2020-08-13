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

import static org.apache.ignite.SqlTracingBenchmarks.BenchmarkContext.QRY_PAGE_SIZE;
import static org.apache.ignite.SqlTracingBenchmarks.BenchmarkContext.SELECT_RANGE;
import static org.apache.ignite.SqlTracingBenchmarks.BenchmarkContext.TABLE_POPULATION;
import static org.apache.ignite.internal.processors.tracing.Tracing.CLIENT_SERVER_1_LOG;
import static org.apache.ignite.internal.processors.tracing.Tracing.SERVER_1_LOG;
import static org.apache.ignite.internal.processors.tracing.Tracing.SERVER_2_LOG;

/** */
public class SqlTracingBenchmarks {
    /** */
    @State(Scope.Benchmark)
    public static class BenchmarkContext {
        /** */
        public static final int NODES_CNT = 2;

        /** */
        public static final int TABLE_POPULATION = 2000;

        /** */
        public static final int SELECT_RANGE = 1000;

        /** */
        public static final int QRY_PAGE_SIZE = 10;

        /** */
        public static final int UPDATE_RANGE = 100;

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

            for (long l = 1; l <= TABLE_POPULATION; ++l) {
                qryProc.querySqlFields(
                    new SqlFieldsQuery("INSERT INTO test_table (id, val) VALUES (?, ?)").setArgs(l, l),
                    true
                );
            }
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
                //.setTracingSpi(new OpenCensusTracingSpi())
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
    @Measurement(iterations = 4, time = 30)
    @Fork(value = 3)
    public void benchmarkSelect(BenchmarkContext ctx) {
        SERVER_1_LOG.clear();
        SERVER_2_LOG.clear();
        CLIENT_SERVER_1_LOG.clear();
        CLIENT_SERVER_1_LOG.clear();

        long lowId = ThreadLocalRandom.current().nextLong(TABLE_POPULATION - SELECT_RANGE);

        long highId = lowId + SELECT_RANGE;

        try (
            FieldsQueryCursor<List<?>> cursor = ctx.clientNode()
                .context().query().querySqlFields(
                    new SqlFieldsQuery("SELECT id, val FROM test_table WHERE id BETWEEN ? and ?")
                        .setArgs(lowId, highId)
                        .setPageSize(QRY_PAGE_SIZE),
                    false
                )
        ) {
            cursor.iterator().forEachRemaining(val -> {});
        }
    }

    /** */
    /*@Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Warmup(iterations = 1, time = 10)
    @Measurement(iterations = 5, time = 10)
    @Fork(value = 3)
    public void benchmarkUpdate(BenchmarkContext ctx) {
        long lowId = ThreadLocalRandom.current().nextLong(TABLE_POPULATION - UPDATE_RANGE);

        long highId = lowId + UPDATE_RANGE;

        try (
            FieldsQueryCursor<List<?>> ignored = ctx.clientNode()
                .context().query().querySqlFields(
                    new SqlFieldsQuery("UPDATE test_table SET val = (val + 1) WHERE id BETWEEN ? AND ?")
                        .setArgs(lowId, highId)
                        .setPageSize(QRY_PAGE_SIZE),
                    false
                )
        ) {
            // No-op.
        }
    }*/
}
