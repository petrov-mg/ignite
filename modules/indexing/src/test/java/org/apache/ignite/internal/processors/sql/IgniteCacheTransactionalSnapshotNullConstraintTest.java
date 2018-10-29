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

package org.apache.ignite.internal.processors.sql;

import java.io.Serializable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 *
 */
public class IgniteCacheTransactionalSnapshotNullConstraintTest extends GridCommonAbstractTest {
    /** */
    private static final String REPLICATED_CACHE_NAME = "replicatedCacheName";

    /** */
    private static final String PARTITIONED_CACHE_NAME = "partitionedCacheName";

    /** */

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);

        jcache(grid(0), cacheConfiguration(REPLICATED, TRANSACTIONAL_SNAPSHOT), REPLICATED_CACHE_NAME);

        jcache(grid(0), cacheConfiguration(PARTITIONED, TRANSACTIONAL_SNAPSHOT), PARTITIONED_CACHE_NAME);

    }

    /** */
    protected CacheConfiguration cacheConfiguration(CacheMode cacheMode, CacheAtomicityMode atomicityMode) {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(cacheMode);
        cfg.setAtomicityMode(atomicityMode);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        QueryEntity qe = new QueryEntity(new QueryEntity(Integer.class, Person.class));

        cfg.setQueryEntities(F.asList(qe));

        return cfg;
    }

    /** @throws Exception If failed.*/
    public void testPutNullValueReplicatedModeFail() throws Exception {
        IgniteCache<Integer, Person> cache = jcache(0, REPLICATED_CACHE_NAME);

        assertThrowsWithCause(() -> {
            cache.put(0, new Person(null, 25));
        }, IgniteException.class);
    }

    /** @throws Exception If failed.*/
    public void testPutNullValuePartitionedModeFail() throws Exception {
        IgniteCache<Integer, Person> cache = jcache(0, PARTITIONED_CACHE_NAME);

        assertThrowsWithCause(() -> {
            cache.put(1, new Person(null, 18));
        }, IgniteException.class);
    }

    /** */
    public static class Person implements Serializable {
        /** */
        @QuerySqlField(notNull = true)
        private String name;

        /** */
        @QuerySqlField
        private int age;

        /** */
        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }
}
