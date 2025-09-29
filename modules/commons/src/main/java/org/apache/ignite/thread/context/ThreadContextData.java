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

package org.apache.ignite.thread.context;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import org.apache.ignite.internal.util.typedef.F;

/** */
class ThreadContextData {
    /** */
    public static final int DFLT_SCOPE_STACK_SIZE = 2;

    /** */
    private int scopeLvl;

    /** */
    private int activeAttrsCnt;

    /** */
    private final ArrayList<Deque<ScopedAttributeValue>> attrs = new ArrayList<>(ThreadContextAttributeRegistry.instance().size());

    /** */
    <T> T get(ThreadContextAttribute<T> attr) {
        if (activeAttrsCnt == 0)
            return attr.defaultValue();

        Deque<ScopedAttributeValue> attrScopedVals = attributeScopedValues(attr.id());

        return F.isEmpty(attrScopedVals)
            ? attr.defaultValue()
            : attrScopedVals.peek().value();
    }

    /** */
    <T> void put(ThreadContextAttribute<T> attr, T val) {
        if (get(attr) == val)
            return;

        Deque<ScopedAttributeValue> attrScopedVals = attributeScopedValues(attr.id());

        if (attrScopedVals == null) {
            attrScopedVals = new ArrayDeque<>(DFLT_SCOPE_STACK_SIZE);

            attrs.add(attr.id(), attrScopedVals);
        }

        if (attrScopedVals.isEmpty())
            ++activeAttrsCnt;
        else if (attrScopedVals.peek().scopeLevel() == scopeLvl)
            throw new UnsupportedOperationException("Overriding an existing attribute value within a scope is not supported");

        attrScopedVals.push(new ScopedAttributeValue(scopeLvl, val));
    }

    /** */
    public ThreadContextSnapshotRecord createSnapshot() {
        ThreadContextSnapshotRecord snapshotRec = ThreadContextSnapshotRecord.EMPTY;

        for (int attrId = attrs.size() - 1; attrId >= 0; attrId--) {
            Deque<ScopedAttributeValue> attrScopedVals = attrs.get(attrId);

            if (F.isEmpty(attrScopedVals))
                continue;

            snapshotRec = new ThreadContextSnapshotRecord(attrId, attrScopedVals.peek().value(), snapshotRec);
        }

        return snapshotRec;
    }

    /** */
    void onScopeCreated() {
        ++scopeLvl;
    }

    /** */
    void onScopeClosed() {
        if (activeAttrsCnt != 0)
            clearScopeData(scopeLvl);

        --scopeLvl;
    }

    /** */
    private void clearScopeData(int scopeLvl) {
        for (Deque<ScopedAttributeValue> attrScopedVals : attrs) {
            if (F.isEmpty(attrScopedVals) || attrScopedVals.peek().scopeLevel() != scopeLvl)
                continue;

            attrScopedVals.pop();

            if (attrScopedVals.isEmpty())
                --activeAttrsCnt;
        }
    }

    /** */
    private Deque<ScopedAttributeValue> attributeScopedValues(int id) {
        return id < attrs.size() ? attrs.get(id) : null;
    }

    /** */
    private static class ScopedAttributeValue {
        /** */
        private final int scopeLvl;

        /** */
        private final Object val;

        /** */
        public ScopedAttributeValue(int scopeLvl, Object val) {
            this.scopeLvl = scopeLvl;
            this.val = val;
        }

        /** */
        public int scopeLevel() {
            return scopeLvl;
        }

        /** */
        public <T> T value() {
            return (T)val;
        }
    }
}
