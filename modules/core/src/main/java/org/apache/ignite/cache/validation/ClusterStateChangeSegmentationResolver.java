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

package org.apache.ignite.cache.validation;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.cache.ExchangeActions;
import org.apache.ignite.internal.processors.cache.StateChangeRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.apache.ignite.thread.OomExceptionHandler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.UNDEFINED;

/**
 * Represents {@link PluggableSegmentationResolver} implementation that detects cluster nodes segmentation and
 * changes state of segmented part of the cluster to read-only mode.
 */
public class ClusterStateChangeSegmentationResolver implements PluggableSegmentationResolver, DistributedMetastorageLifecycleListener {
    /** Ignite kernel context. */
    private final GridKernalContext ctx;

    /** Ignite logger. */
    private final IgniteLogger log;

    /** */
    private final IgniteThreadPoolExecutor exec;

    /** */
    private final Lock topSegmentationStateLock = new ReentrantLock();

    /** */
    private final Condition topSegmentationStateUpdated = topSegmentationStateLock.newCondition();

    /** */
    private IgniteBiTuple<GridDhtPartitionExchangeId, Boolean> topSegmentationState;

    /** */
    private long lastCheckedTopVer;

    /** */
    private int lastCheckedBaselineNodesCnt;

    /** State of the current segment. */
    private volatile boolean isValid;

    /** */
    private static final String SEGMENTATION_STATE_KEY = "segmentation.state";

    /** @param ctx Ignite kernel context. */
    public ClusterStateChangeSegmentationResolver(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        exec = new IgniteThreadPoolExecutor(
            "segmentation-resolver-executor",
            ctx.igniteInstanceName(),
            1,
            1,
            DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            UNDEFINED,
            new OomExceptionHandler(ctx));

        exec.allowCoreThreadTimeOut(true);
    }

    /** {@inheritDoc} */
    @Override public void onDoneBeforeTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
        long exchResultTopVer = fut.topologyVersion().topologyVersion();

        if (lastCheckedTopVer == exchResultTopVer)
            return;

        boolean isValid0 = isValid;

        boolean segStateChanged = false;

        if (lastCheckedTopVer == 0)
            isValid0 = U.isLocalNodeCoordinator(ctx.discovery()) || awaitTopologySegmentationState(exchResultTopVer);
        else {
            if (isValid0) {
                if (segmentationDetected(fut)) {
                    isValid0 = false;

                    segStateChanged = true;

                    U.warn(log, "Cluster segmentation was detected [segmentedNodes=" + formatTopologyNodes(fut) + ']');
                }
            }
            else if (segmentationResolved(fut)) {
                isValid0 = true;

                U.warn(log, "Segmentation sign was removed from nodes [nodes=" + formatTopologyNodes(fut) + ']');
            }
        }

        if (U.isLocalNodeCoordinator(ctx.discovery())) {
            propagateSegmentationState(exchResultTopVer, isValid0);

            if (segStateChanged)
                handleSegmentation();
        }

        lastCheckedTopVer = exchResultTopVer;
        lastCheckedBaselineNodesCnt = fut.events().discoveryCache().aliveBaselineNodes().size();

        isValid = isValid0;
    }

    /** */
    private boolean segmentationDetected(GridDhtPartitionsExchangeFuture fut) {
        int failedBaselineNodes = 0;

        DiscoCache discoCache = fut.events().discoveryCache();

        for (DiscoveryEvent event : fut.events().events()) {
            if (event.type() == EVT_NODE_FAILED && !event.eventNode().isClient() && discoCache.baselineNode(event.node()))
                ++failedBaselineNodes;
        }

        return failedBaselineNodes >= lastCheckedBaselineNodesCnt - lastCheckedBaselineNodesCnt / 2;
    }

    /** */
    private boolean awaitTopologySegmentationState(long topVer) {
        topSegmentationStateLock.lock();

        try {
            while (topSegmentationState != null)
                topSegmentationStateUpdated.await(ctx.config().getFailureDetectionTimeout(), MILLISECONDS);

            return topSegmentationState.getValue();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteException(e);
        }
        finally {
            topSegmentationStateLock.unlock();
        }
    }

    /** */
    private void propagateSegmentationState(long topVer, boolean isValid) {
        exec.submit(() -> {
            try {
                ctx.distributedMetastorage().write(
                    SEGMENTATION_STATE_KEY,
                    new IgniteBiTuple<>(topVer, isValid)
                );
            }
            catch (IgniteCheckedException e) {
                U.error(log, e);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean isValidSegment() {
        return isValid;
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
        metastorage.listen(SEGMENTATION_STATE_KEY::equals, (key, oldVal, newVal) -> {
            topSegmentationStateLock.lock();

            try {
                topSegmentationStateUpdated.signalAll();
            }
            finally {
                topSegmentationStateLock.unlock();
            }
        });
    }

    /** Restricts segmented cluster writes by changing its state to READ-ONLY mode. */
    private void handleSegmentation() {
        exec.submit(() -> {
            try {
                ctx.state().changeGlobalState(
                    ACTIVE_READ_ONLY,
                    false,
                    null,
                    false
                ).get();
            }
            catch (Throwable e) {
                U.error(log, "Failed to switch state of the segmented cluster nodes to the READ-ONLY mode.", e);
            }
        });
    }

    /** */
    private boolean segmentationResolved(GridDhtPartitionsExchangeFuture fut) {
        ExchangeActions exchActions = fut.exchangeActions();

        StateChangeRequest stateChangeReq = exchActions == null ? null : exchActions.stateChangeRequest();

        return stateChangeReq != null && stateChangeReq.state() == ACTIVE;
    }

    /**
     * @return String representation of the topology nodes that are part of the cluster at the end of the specified PME
     * future.
     */
    private String formatTopologyNodes(GridDhtPartitionsExchangeFuture exchFut) {
        return exchFut.events().lastEvent().topologyNodes().stream()
            .map(n -> n.id().toString())
            .collect(Collectors.joining(", "));
    }
}
