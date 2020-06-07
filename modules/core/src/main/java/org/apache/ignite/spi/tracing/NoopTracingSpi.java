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

package org.apache.ignite.spi.tracing;

import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiConsistencyChecked;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Noop and null-safe implementation of Tracing SPI.
 */
@IgniteSpiMultipleInstancesSupport(value = true)
@IgniteSpiConsistencyChecked(optional = true)
public class NoopTracingSpi extends IgniteSpiAdapter implements TracingSpi {
    @Override public SpiSpecificSpan create(@NotNull String name) {
        return NoopSpiSpecificSpan.INSTANCE;
    }

    /** {@inheritDoc} */
    @Override public SpiSpecificSpan create(@NotNull String name, @Nullable byte[] serializedSpan) {
        return NoopSpiSpecificSpan.INSTANCE;
    }


    /** {@inheritDoc} */
    @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public TracingSpiType type() {
        return TracingSpiType.NOOP_TRACING_SPI;
    }
}