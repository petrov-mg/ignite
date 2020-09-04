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

import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageRequest;
import org.apache.ignite.internal.processors.security.UserAttributesFactory;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.security.impl.TestAdditionalSecurityProcessor.CLIENT;

/** */
public class TestClass extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        SslContextFactory sslFactory = (SslContextFactory) GridTestUtils.sslFactory();

        cfg.setSslContextFactory(sslFactory);
       /* cfg.setConnectorConfiguration(new ConnectorConfiguration()
            .setSslEnabled(true)
            .setSslClientAuth(true)
            .setSslClientAuth(true)
            .setSslFactory(sslFactory));

        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
            .setSslEnabled(true)
            .setSslClientAuth(true)
            .setUseIgniteSslContextFactory(false)
            .setSslContextFactory(sslFactory));*/


        return cfg;
    }

    /** */
    @Test
    public void test() throws Exception {
        IgniteEx srv = startGrid(0);

        /*IgniteClient cli = Ignition.startClient(new ClientConfiguration()
            .setAddresses(Config.SERVER)
            .setSslContextFactory(GridTestUtils.sslFactory())
            .setSslMode(SslMode.REQUIRED));

        ClientCache<Object, Object> cache = cli.createCache("test-cache");

        cache.put("key", "val");

        assertTrue("val".equals(cache.get("key")));

        assertNotNull(srv.cache("test-cache"));

        cli.query(new SqlFieldsQuery("CREATE TABLE test_table(id INT PRIMARY KEY, val INT)")).getAll();

        cli.query(new SqlFieldsQuery("INSERT INTO test_table(id, val) VALUES (0, 1)")).getAll();*/



        IgniteEx cli = startClientGrid(1);

        GridQueryNextPageRequest msg = new GridQueryNextPageRequest(0, 0, 0, 0, (byte)0);

        CyclicBarrier barrier = new CyclicBarrier(2);

        srv.context().io().addMessageListener(GridTopic.TOPIC_QUERY, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                try {
                    if (msg instanceof GridQueryNextPageRequest)
                        barrier.await();
                }
                catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
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
}
