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

/**
 * Represents interface which must be implemented by all cursors, whose operations should be traced
 * after returning to user.
 */
public interface TraceableCursor {
    /**
     * Sets span to which current cursor belongs to.
     *
     * @param span Span.
     */
    public void span(Span span);

    /**
     * Sets listner of the current cursor close.
     *
     * @param lsnr Cursor close listener.
     */
    public void onCloseListener(Runnable lsnr);
}
