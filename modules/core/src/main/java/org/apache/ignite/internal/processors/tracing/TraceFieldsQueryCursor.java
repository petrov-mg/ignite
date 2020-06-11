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
import java.util.List;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.jetbrains.annotations.NotNull;

/** */
public class TraceFieldsQueryCursor<T> implements FieldsQueryCursor<T> {
    /** */
    private final FieldsQueryCursor<T> cursor;

    /** */
    private final Span span;

    /** */
    public TraceFieldsQueryCursor(FieldsQueryCursor<T> cursor, Span span) {
        this.cursor = cursor;
        this.span = span;
    }

    /** {@inheritDoc} */
    @Override public String getFieldName(int idx) {
        try (MTC.TraceSurroundings ignored = MTC.supportContinual(span)) {
            return cursor.getFieldName(idx);
        }
    }

    /** {@inheritDoc} */
    @Override public int getColumnsCount() {
        try (MTC.TraceSurroundings ignored = MTC.supportContinual(span)) {
            return cursor.getColumnsCount();
        }
    }

    /** {@inheritDoc} */
    @Override public List<T> getAll() {
        try (MTC.TraceSurroundings ignored = MTC.support(span)) {
            return cursor.getAll();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try (MTC.TraceSurroundings ignored = MTC.support(span)) {
            cursor.close();
        }
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<T> iterator() {
        try (MTC.TraceSurroundings ignored = MTC.supportContinual(span)) {
            return new TraceIterator<>(cursor.iterator());
        }
    }
}
