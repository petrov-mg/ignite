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

/** */
class ThreadContextData {
    /** */
    private static final ScopedAttributeValueStack<?>[] EMPTY = new ScopedAttributeValueStack[0];

    /** */
    private int activeScopeDepth;

    /** */
    private int activeAttrsCnt;

    /** */
    private ScopedAttributeValueStack<?>[] attrs = EMPTY;

    /** */
    <T> T get(ThreadContextAttribute<T> attr) {
        if (attr.id() >= attrs.length)
            return attr.initialValue();

        ScopedAttributeValueStack<T> attrVals = (ScopedAttributeValueStack<T>)attrs[attr.id()];

        return attrVals == null ? attr.initialValue() : attrVals.peek();
    }

    /** */
    <T> void put(ThreadContextAttribute<T> attr, T val) {
        if (attr.id() >= attrs.length)
            grow(attr.id() + 1);

        ScopedAttributeValueStack<T> attrVals = (ScopedAttributeValueStack<T>)attrs[attr.id()];

        if (attrVals == null)
            attrs[attr.id()] = attrVals = new ScopedAttributeValueStack<>(attr);

        push(attrVals, val);
    }

    /** */
    ThreadContextSnapshot createSnapshot() {
        if (activeAttrsCnt == 0)
            return ThreadContextSnapshot.emptySnapshot();

        ThreadContextSnapshot snapshot = ThreadContextSnapshot.emptySnapshot();

        for (int i = 0; i < attrs.length; i++) {
            ScopedAttributeValueStack<?> attrVals = attrs[i];

            if (attrVals != null)
                snapshot = attrVals.exportTopTo(snapshot);
        }

        return snapshot;
    }

    /** */
    void restoreSnapshot(ThreadContextSnapshot snapshot) {
        if (snapshot.isEmpty() && activeAttrsCnt == 0)
            return;

        int maxAttrId = snapshot.isEmpty() ? attrs.length - 1 : Math.max(snapshot.attribute().id(), attrs.length - 1);

        for (int attrId = maxAttrId; attrId >= 0; attrId--) {
            if (!snapshot.isEmpty() && snapshot.attribute().id() == attrId) {
                put(snapshot.attribute(), snapshot.attributeValue());

                snapshot = snapshot.previous();
            }
            else {
                ScopedAttributeValueStack<Object> attrVals = (ScopedAttributeValueStack<Object>)attrs[attrId];

                if (attrVals != null)
                    push(attrVals, attrVals.initialValue());
            }
        }
    }

    /** */
    void onScopeCreated() {
        ++activeScopeDepth;
    }

    /** */
    void onScopeClosed() {
        assert activeScopeDepth != 0;

        if (activeAttrsCnt != 0)
            clearActiveScopeData();

        --activeScopeDepth;
    }

    /** */
    private <T> void push(ScopedAttributeValueStack<T> attrVals, T val) {
        if (attrVals.peek() == val)
            return;

        if (attrVals.isEmpty())
            ++activeAttrsCnt;

        attrVals.push(activeScopeDepth, val);
    }

    /** */
    private void clearActiveScopeData() {
        for (int i = 0; i < attrs.length; i++) {
            ScopedAttributeValueStack<?> attrVals = attrs[i];

            if (attrVals == null)
                continue;

            if (attrVals.pop(activeScopeDepth) && attrVals.isEmpty())
                --activeAttrsCnt;
        }
    }

    /** */
    private void grow(int size) {
        ScopedAttributeValueStack<?>[] upd = new ScopedAttributeValueStack[size];

        if (attrs.length != 0)
            System.arraycopy(attrs, 0, upd, 0, attrs.length);

        attrs = upd;
    }
}
