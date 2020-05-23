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

package org.apache.ignite;

import java.util.List;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class SqlAffinityKeyTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_CACHE = "test-cache";

    /** */
    private static final String TEST_TABLE = "test_table";

    /** */
    @Test
    public void test() throws Exception {
        IgniteEx srv = startGrid(0);

        IgniteCache<?, ?> defaultCache = srv.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setSqlSchema("PUBLIC"));

        defaultCache.query(new SqlFieldsQuery(
            "CREATE TABLE " + TEST_TABLE + "(id INT, affKey INT, val VARCHAR, PRIMARY KEY (id, affKey)) WITH " +
                "   \"template=partitioned" +
                "   ,backups=1" +
                "   ,affinityKey=affKey" +
                "   ,CACHE_NAME=" + TEST_CACHE +
                "   ,KEY_TYPE=" + TestKey.class.getName() +
                "   ,VALUE_TYPE=" + TestValue.class.getName() + "\";"
        )).getAll();

        List<List<?>> res = defaultCache.query(new SqlFieldsQuery(
            "INSERT INTO " + TEST_TABLE + "(id, affKey, val) VALUES (0, 1, 'val');"
        )).getAll();

        assertEquals(1L, res.get(0).get(0));
    }

    /** */
    public static class TestValue {
        /** */
        private String val;

        /** */
        public TestValue(String val) {
            this.val = val;
        }

        /** */
        public String getValue() {
            return val;
        }

        /** */
        public void setValue(String val) {
            this.val = val;
        }
    }

    /** */
    public static class TestKey {
        /** */
        private int id;

        /** */
        @AffinityKeyMapped
        private int affKey;

        /** */
        public TestKey(int id, int affKey) {
            this.id = id;
            this.affKey = affKey;
        }
    }
}