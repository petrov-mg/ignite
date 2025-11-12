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

import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedList;
import org.apache.ignite.internal.util.typedef.F;

/** */
public class ThreadContextSnapshot {
    /** */
    private static final ThreadContextSnapshot EMPTY = new ThreadContextSnapshot(null);

    /** */
    private Collection<Record<?>> records;

    /** */
    private ThreadContextSnapshot(Collection<Record<?>> holder) {
        records = holder;
    }

    /** */
    public Scope restoreAttributesValues() {
        ThreadContextData data = ThreadContextData.get();

        if (isEmpty() && data.activeAttributesCount() == 0)
            return Scope.EMPTY;

        CompositeScope scope = new CompositeScope();

        if (isEmpty())
            data.forEach(attr -> scope.add(attr.applyInitialValue()));
        else {
            BitSet restored = new BitSet();

            for (Record<?> record : records) {
                scope.add(record.restoreAttributeValue());

                restored.set(record.attributeId());
            }

            data.forEach(attr -> {
                if (!restored.get(attr.id()))
                    scope.add(attr.applyInitialValue());
            });
        }

        return scope;
    }

    /** */
    boolean isEmpty() {
        return F.isEmpty(records);
    }

    /** */
    static ThreadContextSnapshot capture() {
        ThreadContextData data = ThreadContextData.get();

        if (data.activeAttributesCount() == 0)
            return EMPTY;

        ThreadContextSnapshot snapshot = new ThreadContextSnapshot(new LinkedList<>());

        data.forEach(snapshot::recordAttributeValue);

        return snapshot;
    }

    /** */
    private <T> void recordAttributeValue(ThreadContextAttribute<T> attr) {
        if (records == null)
            records = new LinkedList<>();

        records.add(new Record<>(attr, attr.value()));
    }

    /** */
    private static class Record<T> {
        /** */
        private final ThreadContextAttribute<T> attr;

        /** */
        private final T attrVal;

        /** */
        Record(ThreadContextAttribute<T> attr, T attrVal) {
            this.attr = attr;
            this.attrVal = attrVal;
        }

        /** */
        int attributeId() {
            return attr.id();
        }

        /** */
        Scope restoreAttributeValue() {
            return attr.applyValue(attrVal);
        }
    }
}
