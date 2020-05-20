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

package org.apache.ignite.thread;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.mxbean.ThreadPoolMXBean;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.ReadOnlyMetricManager;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.spi.systemview.ReadOnlySystemViewRegistry;
import org.apache.ignite.spi.systemview.SystemViewExporterSpi;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.managers.systemview.GridSystemViewManager.STREAM_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.managers.systemview.GridSystemViewManager.SYS_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Tests that thread pool metrics are registered before {@link GridMetricManager#onKernalStart} invocation.
 */
public class ThreadPoolMetricsTest extends GridCommonAbstractTest {
    /** Names of the general thread pools. */
    private static final Collection<String> THREAD_POOL_NAMES = Arrays.asList(
        "GridUtilityCacheExecutor",
        "GridExecutionExecutor",
        "GridServicesExecutor",
        "GridSystemExecutor",
        "GridClassLoadingExecutor",
        "GridManagementExecutor",
        "GridIgfsExecutor",
        "GridAffinityExecutor",
        "GridCallbackExecutor",
        "GridQueryExecutor",
        "GridSchemaExecutor",
        "GridRebalanceExecutor",
        "GridRebalanceStripedExecutor",
        "GridDataStreamExecutor"
    );

    /** Names of the system views for the thread pools. */
    private static final Collection<String> THREAD_POOL_VIEWS = Arrays.asList(
        SYS_POOL_QUEUE_VIEW,
        STREAM_POOL_QUEUE_VIEW
    );

    /** Latch that indicates whether {@link GridMetricManager#onKernalStart} was invoked. */
    public final CountDownLatch startInvokedLatch = new CountDownLatch(1);

    /** Latch that indicates whether {@link GridMetricManager#onKernalStart} execution was unblocked. */
    public final CountDownLatch startUnblockedLatch = new CountDownLatch(1);

    /** Test instance of the {@link MetricExporterSpi}. */
    private final TestMetricExporterSpi metricExporter = new TestMetricExporterSpi();

    /** Test instance of the {@link SystemViewExporterSpi}. */
    private final TestSystemViewExporterSpi sysViewExporter = new TestSystemViewExporterSpi();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setMetricExporterSpi(metricExporter)
            .setSystemViewExporterSpi(sysViewExporter);
    }

    /**
     * Tests that thread pool metrics are registered before {@link GridMetricManager#onKernalStart} invocation.
     *
     * @throws Exception If failed.
     */
    @Test
    @SuppressWarnings({"Convert2MethodRef", "deprecation"})
    public void testThreadPoolMetrics() throws Exception {
        try {
            runAsync(() -> startGrid());

            assertTrue(startInvokedLatch.await(getTestTimeout(), MILLISECONDS));

            metricExporter.checkMetricsRegistered();

            THREAD_POOL_NAMES.forEach(name -> getMxBean(
                getTestIgniteInstanceName(),
                "Thread Pools",
                name,
                ThreadPoolMXBean.class
            ));

            sysViewExporter.checkSystemViewsRegistered();
        }
        finally {
            startUnblockedLatch.countDown();
        }
    }

    /** */
    private static class TestSystemViewExporterSpi extends IgniteSpiAdapter implements SystemViewExporterSpi {
        /** System view registry. */
        private volatile ReadOnlySystemViewRegistry reg;

        /** {@inheritDoc} */
        @Override public void setSystemViewRegistry(ReadOnlySystemViewRegistry reg) {
            this.reg = reg;
        }

        /**
         * Checks that thread pool system views are registered.
         */
        public void checkSystemViewsRegistered() {
            List<String> views = new ArrayList<>();

            reg.forEach(view -> views.add(view.name()));

            assertTrue(views.containsAll(THREAD_POOL_VIEWS));
        }

        /** {@inheritDoc} */
        @Override public void setExportFilter(Predicate<SystemView<?>> filter) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }
    }

    /** */
    private class TestMetricExporterSpi extends IgniteSpiAdapter implements MetricExporterSpi {
        /** Metric registry. */
        private volatile ReadOnlyMetricManager reg;

        /** {@inheritDoc} */
        @Override public void setMetricRegistry(ReadOnlyMetricManager reg) {
            this.reg = reg;
        }

        /** {@inheritDoc} */
        @Override protected void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
            startInvokedLatch.countDown();

            try {
                startUnblockedLatch.await(getTestTimeout(), MILLISECONDS);
            }
            catch (InterruptedException e) {
                throw new IgniteSpiException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void setExportFilter(Predicate<ReadOnlyMetricRegistry> filter) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }

        /**
         * Checks that thread pool metrics are registered.
         */
        public void checkMetricsRegistered() {
            List<String> metrics = new ArrayList<>();

            reg.forEach(metric -> metrics.add(metric.name()));

            assertTrue(metrics.containsAll(THREAD_POOL_NAMES.stream()
                .map(name -> metricName(GridMetricManager.THREAD_POOLS, name))
                .collect(Collectors.toList()))
            );

            assertTrue(metrics.contains(GridMetricManager.IGNITE_METRICS));
        }
    }
}
