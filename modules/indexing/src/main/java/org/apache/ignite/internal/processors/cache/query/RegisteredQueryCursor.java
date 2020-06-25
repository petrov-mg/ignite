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
 *
 */

package org.apache.ignite.internal.processors.cache.query;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.RunningQueryManager;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;
import org.apache.ignite.internal.processors.tracing.NoopSpan;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanType;
import org.apache.ignite.internal.processors.tracing.TraceableCursor;
import org.apache.ignite.internal.processors.tracing.TraceableIterator;
import org.apache.ignite.internal.processors.tracing.Tracing;

import static org.apache.ignite.internal.processors.tracing.SpanTags.ERROR;

/**
 * Query cursor for registered as running queries.
 *
 * Running query will be unregistered during close of cursor.
 */
public class RegisteredQueryCursor<T> extends QueryCursorImpl<T> implements TraceableCursor {
    /** */
    private final AtomicBoolean unregistered = new AtomicBoolean(false);

    /** */
    private RunningQueryManager runningQryMgr;

    /** */
    private Long qryId;

    /** Exception caused query failed or {@code null} if it succeded. */
    private Exception failReason;

    /** Tracing. */
    private final Tracing tracing;

    /** Span which represents query to which current cursor belongs to. */
    private Span qrySpan = NoopSpan.INSTANCE;

    /** Cursor close listener. */
    private volatile Runnable closeLsnr;

    /**
     * @param iterExec Query executor.
     * @param cancel Cancellation closure.
     * @param runningQryMgr Running query manager.
     * @param lazy Lazy mode flag.
     * @param qryId Registered running query id.
     */
    public RegisteredQueryCursor(Iterable<T> iterExec, GridQueryCancel cancel, RunningQueryManager runningQryMgr,
        boolean lazy, Long qryId, Tracing tracing) {
        super(iterExec, cancel, true, lazy);

        assert runningQryMgr != null;
        assert qryId != null;

        this.runningQryMgr = runningQryMgr;
        this.qryId = qryId;
        this.tracing = tracing;
    }

    /** {@inheritDoc} */
    @Override protected Iterator<T> iter() {
        try (TraceSurroundings ignored = MTC.support(tracing.create(SpanType.SQL_CURSOR_ITERATOR_CALL, qrySpan))) {
            Iterator<T> iter = lazy() ? new RegisteredIterator(super.iter()) : super.iter();

            return new TraceableIterator<>(iter, qrySpan);
        }
        catch (Exception e) {
            failReason = e;

            qrySpan.addTag(ERROR, e::getMessage);

            if (QueryUtils.wasCancelled(failReason))
                unregisterQuery();

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try (TraceSurroundings ignored = MTC.support(tracing.create(SpanType.SQL_CURSOR_CLOSE_CALL, qrySpan))) {
            super.close();

            unregisterQuery();

            Runnable lsnr = closeLsnr;

            if (lsnr != null)
                lsnr.run();
        }
    }

    /**
     * Cancels query.
     */
    public void cancel() {
        if (failReason == null)
            failReason = new QueryCancelledException();

        close();
    }

    /**
     * Unregister query.
     */
    private void unregisterQuery() {
        if (unregistered.compareAndSet(false, true))
            runningQryMgr.unregister(qryId, failReason);
    }

    /** {@inheritDoc} */
    @Override public void span(Span span) {
        qrySpan = span;
    }

    /** {@inheritDoc} */
    @Override public void onCloseListener(Runnable lsnr) {
        closeLsnr = lsnr;
    }

    /**
     *
     */
    private class RegisteredIterator implements Iterator<T> {
        /** Delegate iterator. */
        final Iterator<T> delegateIt;

        /**
         * @param it Result set iterator.
         */
        private RegisteredIterator(Iterator<T> it) {
            delegateIt = it;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            try {
                return delegateIt.hasNext();
            }
            catch (Exception e) {
                failReason = e;

                if (QueryUtils.wasCancelled(failReason))
                    unregisterQuery();

                throw e;
            }
        }

        /** {@inheritDoc} */
        @Override public T next() {
            try {
                return delegateIt.next();
            }
            catch (Exception e) {
                failReason = e;

                if (QueryUtils.wasCancelled(failReason))
                    unregisterQuery();

                throw e;
            }
        }
    }
}
