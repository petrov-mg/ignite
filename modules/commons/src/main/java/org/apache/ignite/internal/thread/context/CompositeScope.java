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

import java.util.Collection;
import java.util.LinkedList;
import org.apache.ignite.internal.util.typedef.F;

/** */
class CompositeScope implements Scope {
    /** */
    private Collection<Scope> scopes;

    /** */
    void add(Scope scope) {
        if (scopes == null)
            scopes = new LinkedList<>();

        scopes.add(scope);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (F.isEmpty(scopes))
            return;

        for (Scope scope : scopes)
            scope.close();
    }
}
