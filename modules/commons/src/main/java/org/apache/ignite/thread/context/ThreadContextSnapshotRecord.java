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

/** */
public class ThreadContextSnapshotRecord {
    /** */
    static final ThreadContextSnapshotRecord EMPTY = new ThreadContextSnapshotRecord(-1, null, null);

    /** */
    private final int attrId;

    /** */
    private final Object val;

    /** */
    private final ThreadContextSnapshotRecord next;

    /** */
    public ThreadContextSnapshotRecord(int attrId, Object val, ThreadContextSnapshotRecord next) {
        this.attrId = attrId;
        this.val = val;
        this.next = next == null ? this : next;
    }

    /** */
    public int attributeId() {
        return attrId;
    }

    /** */
    public <T> T value() {
        return (T)val;
    }

    /** */
    public ThreadContextSnapshotRecord next() {
        return next;
    }

    /** */
    public boolean isEmpty() {
        return next == this;
    }
}
