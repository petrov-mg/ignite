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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgniteCacheRemoveTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_CACHE_NAME = "testCache";


    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);

        jcache(grid(0), defaultCacheConfiguration(), TEST_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgniteCacheRemove() throws Exception {
        IgniteCache<TestClass, Object> cache = jcache(0, TEST_CACHE_NAME);

        assertEquals(cache.size(), 0);

        Map<Integer, Boolean> map = new HashMap<>();

        map.put(1, false);
        map.put(2, false);
        map.put(3, false);
        map.put(4, false);
        map.put(5, false);
        map.put(6, false);
        map.put(7, false);
        map.put(8, false);
        map.put(9, false);
        map.put(10, false);
        map.put(19, false);
        map.put(20, false);

        TestClass testCls = new TestClass(map);

        cache.put(testCls, new Object());

        assertEquals(cache.size(), 1);

        final Iterator<IgniteCache.Entry<TestClass, Object>> iter = cache.iterator();

        TestClass k = iter.next().getKey();

        assertEquals(k, testCls);
        assertEquals(k.hashCode(), testCls.hashCode());

        assertTrue(cache.containsKey(new TestClass(map)));
        assertTrue(cache.containsKey(testCls));
        assertTrue(cache.containsKey(k));
        assertTrue(cache.remove(k));

        assertFalse(cache.containsKey(k));
    }

    /** */
    private class TestClass {
        /** */
        public Map<Integer, Boolean> map;

        /** */
        public TestClass(Map<Integer, Boolean> map) {
            this.map = new HashMap<>(map);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof TestClass))
                return false;

            TestClass cls = (TestClass)o;

            return Objects.equals(map, cls.map);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(map);
        }
    }
}
