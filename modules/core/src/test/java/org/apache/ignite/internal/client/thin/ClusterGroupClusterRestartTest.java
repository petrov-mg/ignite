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

package org.apache.ignite.internal.client.thin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import org.apache.ignite.client.ClientClusterGroup;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction.IDX_ATTR;

/** */
public class ClusterGroupClusterRestartTest extends AbstractThinClientTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setUserAttributes(Collections.singletonMap(IDX_ATTR, getTestIgniteInstanceIndex(igniteInstanceName)));
    }

    /** */
    @Test
    public void testGroupNodesAfterClusterRestart() throws Exception {
        prepareCluster();

        try (IgniteClient client = startClient(0, 1)) {
            Collection<ClusterNode> clusterNodes = G.allGrids().stream().map(g -> g.cluster().localNode()).collect(Collectors.toList());

            ClientClusterGroup dfltGrp = client.cluster();
            ClientClusterGroup srvGrp = client.cluster().forServers();
            ClientClusterGroup cliGrp = client.cluster().forClients();
            ClientClusterGroup attrGrp = client.cluster().forAttribute(IDX_ATTR, 0);
            ClientClusterGroup idGrp = client.cluster().forNodeIds(F.nodeIds(clusterNodes));
            ClientClusterGroup nodeGrp = client.cluster().forNodes(clusterNodes);

            assertContainsNodes(dfltGrp, 0, 1, 2);
            assertContainsNodes(srvGrp, 0, 1);
            assertContainsNodes(cliGrp, 2);
            assertContainsNodes(attrGrp, 0);
            assertContainsNodes(idGrp, 0, 1, 2);
            assertContainsNodes(nodeGrp, 0, 1, 2);

            stopAllGrids();

            prepareCluster();

            assertNotContainsNodes(idGrp, clusterNodes);
            assertNotContainsNodes(nodeGrp, clusterNodes);
            assertContainsNodes(dfltGrp, 0, 1, 2);
            assertContainsNodes(srvGrp, 0, 1);
            assertContainsNodes(cliGrp, 2);
            assertContainsNodes(attrGrp, 0);
        }
    }

    /** */
    private void assertContainsNodes(ClientClusterGroup grp, int... nodeIdxs) {
        assertTrue(grp.nodes().containsAll(Arrays.stream(nodeIdxs).mapToObj(idx -> grid(idx).localNode()).collect(Collectors.toList())));

        for (int idx : nodeIdxs)
            assertNotNull(grp.node(grid(idx).localNode().id()));
    }

    /** */
    private void assertNotContainsNodes(ClientClusterGroup grp, Collection<ClusterNode> nodes) {
        for (ClusterNode node : grp.nodes())
            assertFalse(nodes.contains(node));

        for (ClusterNode node : nodes)
            assertNull(grp.node(node.id()));
    }

    /** */
    private void prepareCluster() throws Exception {
        startGrids(2);
        startClientGrid(2);
    }
}
