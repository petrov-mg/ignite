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
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class CacheStartNoServerNodesTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void test() throws Exception {
        IgniteEx server = startGrid(0);

        server.cluster().state(ClusterState.ACTIVE);

        IgniteEx client = startClientGrid(1);

        UUID clientId = client.localNode().id();

        server.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setNodeFilter(node -> node.id().equals(clientId)));

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setLocalListener(evts -> {
            //No-op.
        });

        client.cache(DEFAULT_CACHE_NAME).query(qry);
    }
}
