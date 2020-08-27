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

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import io.opencensus.exporter.trace.zipkin.ZipkinExporterConfiguration;
import io.opencensus.exporter.trace.zipkin.ZipkinTraceExporter;
import io.opencensus.trace.Tracing;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.log4j.Log4JLogger;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.ignite.spi.tracing.Scope.SQL;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;

/** */
public class TestClass {
    /** */
    public static final int NODES_CNT = 2;

    /** */
    public static final int TABLE_POPULATION = 2000;

    /** */
    public static final int SELECT_RANGE = 1000;

    /** */
    public static final int QRY_PAGE_SIZE = 5;

    /** */
    public static final int UPDATE_RANGE = 100;

    /** */
    @BeforeClass
    public static void before() {
        ZipkinTraceExporter.createAndRegister(ZipkinExporterConfiguration.builder()
            .setV2Url("http://localhost:9411/api/v2/spans")
            .setServiceName("ignite")
            .build());
    }

    /** */
    @Test
    public void test() throws Exception {
        for (int i = 0; i < NODES_CNT; i++)
            startGrid(i, false);

        IgniteEx cli = startGrid(NODES_CNT, true);

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

        cli.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(SQL).build(),
            new TracingConfigurationParameters.Builder()
                .withSamplingRate(SAMPLING_RATE_ALWAYS).build());

        for (int i = 0; i < 1 ; i++) {
            long lowId = ThreadLocalRandom.current().nextLong(TABLE_POPULATION - UPDATE_RANGE);

            long highId = lowId + UPDATE_RANGE;

            try (
                FieldsQueryCursor<List<?>> ignored = cli
                    .context().query().querySqlFields(
                        new SqlFieldsQuery("UPDATE test_table SET val = (val + 1) WHERE id BETWEEN ? AND ?")
                            .setArgs(lowId, highId)
                            .setPageSize(QRY_PAGE_SIZE),
                        false
                    )
            ) {
                // No-op.
            }
        }

        Ignition.stopAll(true);

        U.sleep(5000);

        Tracing.getExportComponent().shutdown();
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
}