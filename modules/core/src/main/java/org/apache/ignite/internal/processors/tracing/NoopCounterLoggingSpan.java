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

/** */
public class NoopCounterLoggingSpan extends AbstractSpanWrapper<CounterLoggingSpan> implements CounterLoggingSpan{
    /** Instance. */
    public static final CounterLoggingSpan INSTANCE = new NoopCounterLoggingSpan();

    /** */
    private NoopCounterLoggingSpan() {
        super(NoopSpan.INSTANCE);
    }

    /** {@inheritDoc} */
    @Override public void incrementCounter(String name) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void logCounters() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public CounterLoggingSpan registerCounter(String name) {
        return getThis();
    }

    /** {@inheritDoc} */
    @Override protected CounterLoggingSpan getThis() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceable() {
        return false;
    }
}
