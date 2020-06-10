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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.util.GridStringBuilder;

/** */
public class CounterLoggingSpanImpl extends AbstractSpanWrapper<CounterLoggingSpan> implements CounterLoggingSpan {
    /** Counter storage. */
    private final Map<String, LongAdder> counters = new ConcurrentHashMap<>();

    /** */
    private CounterLoggingSpanImpl(Span span) {
        super(span);
    }

    /** {@inheritDoc} */
    @Override protected CounterLoggingSpan getThis() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public CounterLoggingSpan end() {
        logCounters();

        return super.end();
    }

    /** {@inheritDoc} */
    @Override public void logCounters() {
        if (counters.isEmpty())
            return;

        GridStringBuilder sb = new GridStringBuilder().a("Statistics [");

        String delimeter = ", ";

        counters.forEach((name, val) -> sb.a(name + '=' + val).a(delimeter));

        sb.setLength(sb.length() - delimeter.length());

        sb.a(']');

        addLog(sb::toString);
    }

    /** {@inheritDoc} */
    @Override public void incrementCounter(String name) {
        LongAdder cntr = counters.get(name);

        if (cntr != null)
            cntr.increment();
    }

    /** {@inheritDoc} */
    @Override public CounterLoggingSpan registerCounter(String name) {
        counters.putIfAbsent(name, new LongAdder());

        return getThis();
    }

    /** */
    public static Span wrap(Span span) {
        return span.isTraceable() ? new CounterLoggingSpanImpl(span) : NoopCounterLoggingSpan.INSTANCE;
    }
}
