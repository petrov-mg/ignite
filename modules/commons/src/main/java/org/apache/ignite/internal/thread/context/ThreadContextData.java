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

import java.util.function.Consumer;

/** */
class ThreadContextData {
    /** */
    private static final ThreadLocal<ThreadContextData> INSTANCE = ThreadLocal.withInitial(ThreadContextData::new);

    /** */
    private static final ScopedAttributeValueStack<?>[] EMPTY = new ScopedAttributeValueStack[0];

    /** */
    private ThreadContextData() {
        // No-op.
    }

    /** */
    private ScopedAttributeValueStack<?>[] attrs = EMPTY;

    /** */
    private int activeAttrsCnt;

    /** */
    <T> T retrieveAttributeValue(int attrId) {
        if (attrId >= attrs.length)
            return null;

        ScopedAttributeValueStack<T> attrVals = (ScopedAttributeValueStack<T>)attrs[attrId];

        return attrVals == null ? null : attrVals.peek();
    }

    /** */
    <T> void storeAttributeValue(ThreadContextAttribute<T> attr, T val) {
        if (attr.id() >= attrs.length)
            grow(attr.id() + 1);

        ScopedAttributeValueStack<T> attrVals = (ScopedAttributeValueStack<T>)attrs[attr.id()];

        if (attrVals == null)
            attrs[attr.id()] = attrVals = new ScopedAttributeValueStack<>(attr);

        if (attrVals.isEmpty())
            ++activeAttrsCnt;

        attrVals.push(val);
    }

    /** */
    void rollbackAttributeValue(int attrId) {
        ScopedAttributeValueStack<?> attrVals = attrs[attrId];

        assert attrVals != null;

        attrVals.pop();

        if (attrVals.isEmpty())
            --activeAttrsCnt;
    }

    /** */
    public int activeAttributesCount() {
        return activeAttrsCnt;
    }

    /** */
    void forEach(Consumer<ThreadContextAttribute<?>> action) {
        for (ScopedAttributeValueStack<?> attrVals : attrs) {
            if (attrVals != null && !attrVals.isEmpty())
                action.accept(attrVals.attribute());
        }
    }

    /** */
    private void grow(int size) {
        ScopedAttributeValueStack<?>[] upd = new ScopedAttributeValueStack[size];

        if (attrs.length != 0)
            System.arraycopy(attrs, 0, upd, 0, attrs.length);

        attrs = upd;
    }

    /** */
    static ThreadContextData get() {
        return INSTANCE.get();
    }
}
