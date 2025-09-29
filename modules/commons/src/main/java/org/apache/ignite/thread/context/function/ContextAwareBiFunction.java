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

package org.apache.ignite.thread.context.function;

import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.ignite.thread.context.ContextAwareWrapper;
import org.apache.ignite.thread.context.Scope;
import org.apache.ignite.thread.context.ThreadContext;
import org.apache.ignite.thread.context.ThreadContextSnapshotRecord;
import org.jetbrains.annotations.NotNull;

public class ContextAwareBiFunction<T, U ,R> extends ContextAwareWrapper<BiFunction<T, U ,R>> implements BiFunction<T, U, R> {
    /** */
    public ContextAwareBiFunction(BiFunction<T, U, R> delegate, ThreadContextSnapshotRecord snapshot) {
        super(delegate, snapshot);
    }

    /** {@inheritDoc} */
    @Override public R apply(T t, U u) {
        try (Scope ignored = ThreadContext.withSnapshot(snapshot)) {
            return delegate.apply(t, u);
        }
    }

    /** {@inheritDoc} */
    @NotNull @Override public <V> BiFunction<T, U, V> andThen(@NotNull Function<? super R, ? extends V> after) {
        return BiFunction.super.andThen(ContextAwareFunction.wrap(after));
    }

    /** */
    public static <T, U, R> BiFunction<T, U ,R> wrap(BiFunction<T, U, R> delegate) {
        return wrap(delegate, ContextAwareBiFunction::new);
    }
}
