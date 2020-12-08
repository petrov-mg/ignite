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

package org.apache.ignite.internal.transactions.proxy;

import org.apache.ignite.transactions.Transaction;

/**
 * Represents {@link TransactionProxy} implementation that uses {@link Transaction} to perform transaction
 * operations.
 */
public class IgniteTransactionProxy implements TransactionProxy {
    /** */
    private final Transaction tx;

    /** */
    public IgniteTransactionProxy(Transaction tx) {
        this.tx = tx;
    }

    /** {@inheritDoc} */
    @Override public void commit() {
        tx.commit();
    }

    /** {@inheritDoc} */
    @Override public void rollback() {
        tx.rollback();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        tx.close();
    }

    /** {@inheritDoc} */
    @Override public boolean setRollbackOnly() {
        return tx.setRollbackOnly();
    }
}
