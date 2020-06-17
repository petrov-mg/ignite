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

import java.util.Iterator;
import java.util.function.Consumer;

/** */
public class TraceableIterator<T> implements Iterator<T> {
    /** */
    private final Iterator<T> iter;

    /** */
    private final Span span;

    /** */
    public TraceableIterator(Iterator<T> iter) {
        this.iter = iter;
        this.span = MTC.span();
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        try (MTC.TraceSurroundings ignored = MTC.supportContinual(span)){
            return iter.hasNext();
        }
    }

    /** {@inheritDoc} */
    @Override public T next() {
        try (MTC.TraceSurroundings ignored = MTC.supportContinual(span)){
            return iter.next();
        }
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        try (MTC.TraceSurroundings ignored = MTC.supportContinual(span)){
            iter.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public void forEachRemaining(Consumer<? super T> action) {
        try (MTC.TraceSurroundings ignored = MTC.supportContinual(span)){
            iter.forEachRemaining(action);
        }
    }
}
