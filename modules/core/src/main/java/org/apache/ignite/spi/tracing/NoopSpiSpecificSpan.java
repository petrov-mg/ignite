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

import java.util.Map;
import org.jetbrains.annotations.NotNull;

/**
 * Noop and null-safe implementation of {@link SpiSpecificSpan}.
 */
public class NoopSpiSpecificSpan implements SpiSpecificSpan {
    /** Noop serialized span. */
    private static final byte[] NOOP_SPI_SPECIFIC_SERIALIZED_SPAN = new byte[0];

    /** Instance. */
    public static final SpiSpecificSpan INSTANCE = new NoopSpiSpecificSpan();

    /**
     * Constructor.
     */
    private NoopSpiSpecificSpan(){

    }

    /** {@inheritDoc} */
    @Override public SpiSpecificSpan addTag(String tagName, String tagVal) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public SpiSpecificSpan addTag(String tagName, long tagVal) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public SpiSpecificSpan addLog(String logDesc) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public SpiSpecificSpan addLog(String logDesc, Map<String, String> attrs) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public SpiSpecificSpan setStatus(SpanStatus spanStatus) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public SpiSpecificSpan end() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public SpiSpecificSpan createChildSpan(@NotNull String name, double samplingRate) {
        return INSTANCE;
    }

    /** {@inheritDoc} */
    @Override public byte[] toByteArray() {
        return NOOP_SPI_SPECIFIC_SERIALIZED_SPAN;
    }

    /** {@inheritDoc} */
    @Override public boolean isEnded() {
        return true;
    }
}
