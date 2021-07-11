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

package org.apache.ignite.internal.processors.security.events;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVTS_TASK_EXECUTION;
import static org.apache.ignite.events.EventType.EVT_TASK_STARTED;

/**
 * Tests that an event's local listener and an event's remote filter get correct subjectId when task's or job's
 * operations are performed.
 */
public class TaskSecurityContextTest extends AbstractSecurityTest {
    /** */
    private static final String TASK_NAME = "org.apache.ignite.internal.processors.security.events.TaskSecurityContextTest$TestComputeTask";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setIncludeEventTypes(EVTS_TASK_EXECUTION)
            .setClientConnectorConfiguration(
                new ClientConnectorConfiguration().setThinClientConfiguration(
                    new ThinClientConfiguration().setMaxActiveComputeTasksPerConnection(1)));
    }

    /** */
    @Test
    public void test() throws Exception {
        IgniteEx ignite = startGridAllowAll("srv");

        String login = "test";

        IgniteClient cli = Ignition.startClient(new ClientConfiguration()
            .setAddresses(Config.SERVER)
            .setUserName(login)
            .setUserPassword("")
        );

        UUID subjId = ignite.context().security().authenticatedSubjects().stream()
            .filter(subj -> subj.login().equals(login))
            .findFirst()
            .get()
            .id();

        CountDownLatch latch = new CountDownLatch(3);

        ignite.events().localListen(evt -> {
            assertEquals(subjId, ((TaskEvent)evt).subjectId());

            latch.countDown();

            return true;
        }, EVTS_TASK_EXECUTION);

        cli.compute().execute(TASK_NAME, subjId);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    /** Test compute task. */
    public static class TestComputeTask extends ComputeTaskAdapter<UUID, Void> {
        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            @Nullable UUID secSubjId
        ) throws IgniteException {
            return F.asMap(new ComputeJob() {
                /** */
                @IgniteInstanceResource
                private IgniteEx ignite;

                @Override public void cancel() {
                    // No-op.
                }

                @Override public Object execute() throws IgniteException {
                    return null;
                }
            }, subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public @Nullable Void reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }
}
