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

package org.apache.ignite.internal.thread.context.timeout;

import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.context.ContextAwareWrapper;
import org.apache.ignite.thread.context.Scope;
import org.apache.ignite.thread.context.ThreadContext;
import org.apache.ignite.thread.context.ThreadContextSnapshotRecord;

/** */
public class ContextAwareTimeoutObject extends ContextAwareWrapper<GridTimeoutObject> implements GridTimeoutObject {
    /** */
    protected ContextAwareTimeoutObject(GridTimeoutObject delegate, ThreadContextSnapshotRecord snapshot) {
        super(delegate, snapshot);
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid timeoutId() {
        return delegate.timeoutId();
    }

    /** {@inheritDoc} */
    @Override public long endTime() {
        return delegate.endTime();
    }

    /** {@inheritDoc} */
    @Override public void onTimeout() {
        try (Scope ignored = ThreadContext.withSnapshot(snapshot)) {
            delegate.onTimeout();
        }
    }

    /** */
    public static ContextAwareTimeoutObject wrap(GridTimeoutObject delegate) {
        return wrap(delegate, ContextAwareTimeoutObject::new);
    }
}
