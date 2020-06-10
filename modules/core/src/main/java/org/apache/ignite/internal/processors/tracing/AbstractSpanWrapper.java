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

import java.util.Set;
import java.util.function.Supplier;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.SpanContext;
import org.apache.ignite.spi.tracing.SpanStatus;

/** */
public abstract class AbstractSpanWrapper<T extends Span> implements Span {
    /** */
    private final Span span;

    /** */
    protected AbstractSpanWrapper(Span span) {
        this.span = span;
    }

    /** */
    protected abstract T getThis();

    /** {@inheritDoc} */
    @Override public T addTag(String tagName, Supplier<String> tagValSupplier) {
        span.addTag(tagName, tagValSupplier);

        return getThis();
    }

    /** {@inheritDoc} */
    @Override public T addLog(Supplier<String> logDescSupplier) {
        span.addLog(logDescSupplier);

        return getThis();
    }

    /** {@inheritDoc} */
    @Override public T setStatus(SpanStatus spanStatus) {
        span.setStatus(spanStatus);

        return getThis();
    }

    /** {@inheritDoc} */
    @Override public T end() {
        span.end();

        return getThis();
    }

    /** {@inheritDoc} */
    @Override public boolean isEnded() {
        return span.isEnded();
    }

    /** {@inheritDoc} */
    @Override public SpanType type() {
        return span.type();
    }

    /** {@inheritDoc} */
    @Override public Set<Scope> includedScopes() {
        return span.includedScopes();
    }

    /** {@inheritDoc} */
    @Override public SpanContext spanContext() {
        return span.spanContext();
    }

    /** {@inheritDoc} */
    @Override public boolean isTraceable() {
        return true;
    }
}
