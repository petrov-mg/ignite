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

package org.apache.ignite.internal.thread.context;

import java.util.concurrent.atomic.AtomicInteger;

/** */
public class ThreadContextAttribute<T> {
    /** */
    private static final AtomicInteger ID_GEN = new AtomicInteger();

    /** */
    private final int id;

    /** */
    private final T initialVal;

    /** */
    private final Scope attrScope;

    /** */
    private ThreadContextAttribute(int id, T initialVal) {
        this.id = id;
        this.initialVal = initialVal;
        this.attrScope = new AttributeValueScope(id);
    }

    /** */
    int id() {
        return id;
    }

    /** */
    public T initialValue() {
        return initialVal;
    }

    /** */
    public T value() {
        T val = ThreadContextData.get().retrieveAttributeValue(id);

        return val == null ? initialVal : val;
    }

    /** */
    public Scope applyValue(T val) {
        if (value() == val)
            return Scope.EMPTY;

        ThreadContextData.get().storeAttributeValue(this, val);

        return attrScope;
    }

    /** */
    Scope applyInitialValue() {
        return applyValue(initialVal);
    }

    /**
     * Creates attribute instance with initial value set to {@code null}.
     *
     * @see #newInstance(Object)
     */
    public static <T> ThreadContextAttribute<T> newInstance() {
        return newInstance(null);
    }

    /** */
    public static <T> ThreadContextAttribute<T> newInstance(T initialVal) {
        return new ThreadContextAttribute<>(ID_GEN.getAndIncrement(), initialVal);
    }
}
