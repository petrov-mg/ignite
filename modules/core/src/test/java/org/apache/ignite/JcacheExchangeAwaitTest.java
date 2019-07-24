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

package org.apache.ignite;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.ATOMICS_CACHE_NAME;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.DEFAULT_VOLATILE_DS_GROUP_NAME;

/** */
public class JcacheExchangeAwaitTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        return cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /** */
    @Test
    public void testJcacheExchangeAwait() throws Exception {
        startGrids(2);

        ignite(0).cluster().active(true);

        awaitPartitionMapExchange();

        Thread fullMsgDelayer = new Thread(() -> {
            TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(ignite(0));

            spi.blockMessages((node, msg) ->  msg instanceof GridDhtPartitionsFullMessage);

            try {
                spi.waitForBlocked();

                U.sleep(1000);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }

            spi.stopBlock();
        });

        fullMsgDelayer.setDaemon(true);
        fullMsgDelayer.start();

        Thread reentrantLockInvoker = new Thread(() ->
            ignite(0).reentrantLock("1", true, true, true));

        reentrantLockInvoker.setDaemon(true);
        reentrantLockInvoker.start();

        U.sleep(500);

        ignite(1).context().cache().jcache(ATOMICS_CACHE_NAME + '@' + DEFAULT_VOLATILE_DS_GROUP_NAME);

        ignite(1).reentrantLock("2", true, true, true);
    }
}
