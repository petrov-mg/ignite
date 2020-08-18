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

package org.apache.ignite.internal.processors.tracing;

import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.BufferUtils;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.ignite.internal.processors.tracing.messages.TraceableMessagesHandler;
import org.apache.ignite.spi.tracing.TracingConfigurationManager;
import org.jetbrains.annotations.NotNull;

/**
 * Tracing sub-system interface.
 */
public interface Tracing extends SpanManager {
    /** */
    public static final Buffer SERVER_1_LOG = BufferUtils.synchronizedBuffer(new CircularFifoBuffer(100));

    /** */
    public static final Buffer SERVER_2_LOG = BufferUtils.synchronizedBuffer(new CircularFifoBuffer(100));

    /** */
    public static final Buffer CLIENT_SERVER_1_LOG = BufferUtils.synchronizedBuffer(new CircularFifoBuffer(100));

    /** */
    public static final Buffer CLIENT_SERVER_2_LOG = BufferUtils.synchronizedBuffer(new CircularFifoBuffer(100));

    public static void log(boolean isClient, Object id, String msg) {
        if (isClient) {
            if ("0".equals(id))
                CLIENT_SERVER_1_LOG.add(msg);
            else if ("1".equals(id))
                CLIENT_SERVER_2_LOG.add(msg);
            else {
                CLIENT_SERVER_2_LOG.add(id + " ----> " + msg);
                CLIENT_SERVER_1_LOG.add(id + " ----> " + msg);
            }
        }
        else {
            if ("0".equals(id))
                SERVER_1_LOG.add(msg);
            else if ("1".equals(id))
                SERVER_2_LOG.add(msg);
            else {
                CLIENT_SERVER_2_LOG.add(id + " ----> " + msg);
                CLIENT_SERVER_1_LOG.add(id + " ----> " + msg);
            }
        }
    }

    /**
     * @return Helper to handle traceable messages.
     */
    public TraceableMessagesHandler messages();

    /**
     * Returns the {@link TracingConfigurationManager} instance that allows to
     * <ul>
     *     <li>Configure tracing parameters such as sampling rate for the specific tracing coordinates
     *          such as scope and label.</li>
     *     <li>Retrieve the most specific tracing parameters for the specified tracing coordinates (scope and label)</li>
     *     <li>Restore the tracing parameters for the specified tracing coordinates to the default.</li>
     *     <li>List all pairs of tracing configuration coordinates and tracing configuration parameters.</li>
     * </ul>
     * @return {@link TracingConfigurationManager} instance.
     */
    public @NotNull TracingConfigurationManager configuration();
}
