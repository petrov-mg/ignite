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

/** */
public class SpanStatisticsImpl implements SpanStatistics {
    /** Counter storage. */
    private final Map<String, LongAdder> cntrs = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public void exportTo(Span span) {
        cntrs.forEach((name, cntr) -> span.addTag(name, cntr::toString));
    }

    /** {@inheritDoc} */
    @Override public void registerCounters(String... names) {
        for (String name : names)
            cntrs.putIfAbsent(name, new LongAdder());
    }

    /** {@inheritDoc} */
    @Override public void incrementCounter(String name) {
        LongAdder cntr = cntrs.get(name);

        if (cntr != null)
            cntr.increment();
    }
}
