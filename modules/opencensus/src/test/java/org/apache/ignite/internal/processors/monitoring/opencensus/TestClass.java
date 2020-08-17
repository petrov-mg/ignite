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

import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageRequest;
import org.apache.ignite.logger.log4j.Log4JLogger;
import org.junit.Test;

import static org.junit.Assert.fail;

/** */
public class TestClass {
    /** */
    @Test
    public void test() throws Exception {
        IgniteEx srv = startGrid(0, false);

        IgniteEx cli = startGrid(1, true);

        GridQueryNextPageRequest msg = new GridQueryNextPageRequest(0, 0, 0, 0, (byte)0);

        CyclicBarrier barrier = new CyclicBarrier(2);

        srv.context().io().addMessageListener(GridTopic.TOPIC_QUERY, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                try {
                    if (msg instanceof GridQueryNextPageRequest)
                        barrier.await();
                }
                catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }
            }
        });

        for (int i = 0; i < 1000; i++) {
            barrier.reset();

            cli.context().io().sendToGridTopic(srv.context().discovery().localNode(), GridTopic.TOPIC_QUERY, msg, GridIoPolicy.QUERY_POOL);

            try {
                barrier.await(1, TimeUnit.SECONDS);
            }
            catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                fail();
            }
        }
    }

    /** */
    private IgniteEx startGrid(int idx, boolean clientMode) throws Exception {
        return (IgniteEx) Ignition.start(new IgniteConfiguration()
            .setIgniteInstanceName("node-" + idx)
            .setGridLogger(new Log4JLogger("modules/core/src/test/config/log4j-test.xml"))
            .setClientMode(clientMode));
    }
}