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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.tracing.CounterLoggingSpan;
import org.apache.ignite.internal.processors.tracing.CounterLoggingSpanImpl;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;
import org.apache.ignite.internal.processors.tracing.Tracing;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.apache.ignite.spi.tracing.TracingSpi;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.ignite.internal.processors.tracing.SpanType.DISCOVERY_NODE_FAILED;
import static org.apache.ignite.spi.tracing.Scope.DISCOVERY;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;

/** */
public class TracingTest extends AbstractTracingTest {
    /** */
    @BeforeClass
    public static void beforeTests() {
        ZipkinTraceExporter.createAndRegister(ZipkinExporterConfiguration.builder()
            .setV2Url("http://localhost:9411/api/v2/spans")
            .setServiceName("ignite")
            .build());
    }

    /** {@inheritDoc} */
    @Override protected TracingSpi getTracingSpi() {
        return new OpenCensusTracingSpi();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setTracingSpi(new OpenCensusTracingSpi())
            .setTransactionConfiguration(new TransactionConfiguration());
    }

    /** */
    @Test
    public void test() throws IgniteInterruptedCheckedException {
        IgniteEx srv = ignite(0);

        srv.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(DISCOVERY).build(),
            new TracingConfigurationParameters.Builder()
                .withSamplingRate(SAMPLING_RATE_ALWAYS)
                .build());

        try (TraceSurroundings ignored = MTC.support(CounterLoggingSpanImpl.wrap(tracing(srv).create(DISCOVERY_NODE_FAILED)))) {
            MTC.<CounterLoggingSpan>span().registerCounter("cntr").registerCounter("cntr2");

            MTC.<CounterLoggingSpan>span().incrementCounter("cntr");
            MTC.<CounterLoggingSpan>span().incrementCounter("cntr");

            try (TraceSurroundings ignored1 = MTC.support(CounterLoggingSpanImpl.wrap(tracing(srv).create(DISCOVERY_NODE_FAILED, MTC.span())))) {
                MTC.<CounterLoggingSpan>span().registerCounter("cntr").registerCounter("cntr1");

                MTC.<CounterLoggingSpan>span().incrementCounter("cntr");
                MTC.<CounterLoggingSpan>span().incrementCounter("cntr");
            }

            MTC.<CounterLoggingSpan>span().incrementCounter("cntr2");
            MTC.<CounterLoggingSpan>span().incrementCounter("cntr2");
        }

        handler().flush();
    }

    /** */
    Tracing tracing(IgniteEx ignte) {
        return ignte.context().tracing();
    }
}