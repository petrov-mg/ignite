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
import org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;

import static org.apache.ignite.internal.processors.tracing.SpanTags.ERROR;

/**
 * Represents wrapper which gives an ability to execute iterator methods in the specified trace context.
 */
public class TraceableIterator<T> implements Iterator<T> {
    /** Iterator to which all method calls will be delegated. */
    private final Iterator<T> iter;

    /** Span in which context method should be executed. */
    private final Span span;

    /**
     * @param iter Iterator to which all method calls will be delegated.
     * @param span Span in which context method should be executed.
     */
    public TraceableIterator(Iterator<T> iter, Span span) {
        this.iter = iter;
        this.span = span;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        try (TraceSurroundings ignored = MTC.supportContinual(span)) {
            return iter.hasNext();
        }
        catch (Throwable e) {
            span.addTag(ERROR, e::getMessage);

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public T next() {
        try (TraceSurroundings ignored = MTC.supportContinual(span)) {
            return iter.next();
        }
        catch (Throwable e) {
            span.addTag(ERROR, e::getMessage);

            throw e;
        }
    }
}
