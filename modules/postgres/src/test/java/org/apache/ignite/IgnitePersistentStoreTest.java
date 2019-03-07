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

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import java.lang.instrument.Instrumentation;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.jdbc.CacheAbstractJdbcStore;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStore;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.cache.store.jdbc.JdbcTypeField;
import org.apache.ignite.cache.store.jdbc.dialect.BasicJdbcDialect;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.log4j.Log4JLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.tests.pojos.Person;
import org.apache.ignite.tests.pojos.PersonId;
import org.apache.ignite.tests.utils.TestCacheSession;
import org.apache.ignite.tests.utils.TestsHelper;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import sun.instrument.InstrumentationImpl;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/** */
public class IgnitePersistentStoreTest extends GridCommonAbstractTest {
    /** */
    private static final Logger log = Logger.getLogger(IgnitePersistentStoreTest.class.getName());

    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private static EmbeddedPostgres pg;

    /** */
    protected CacheStoreSession ses = new TestCacheSession(CACHE_NAME);

    /**
     * @param store Store.
     */
    protected void inject(CacheJdbcPojoStore store) {
        GridTestUtils.setFieldValue(store, CacheAbstractJdbcStore.class,"log", new Log4JLogger(log));

        GridTestUtils.setFieldValue(store, CacheAbstractJdbcStore.class,"ignite", grid());

        GridTestUtils.setFieldValue(store, CacheAbstractJdbcStore.class,"ses", ses);
    }

    /** */
    @BeforeClass
    public static void setUpClass() {
        try {
            pg = EmbeddedPostgres.start();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to start embedded Postgres instance", e);
        }
    }

    /** */
    @AfterClass
    public static void tearDownClass() {
        try {
            pg.close();
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to stop embedded Postgres instance", e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Connection conn = pg.getPostgresDatabase().getConnection();

        conn.setAutoCommit(false);

        Statement stmt = conn.createStatement();

        stmt.executeUpdate("DROP TABLE IF EXISTS LongCache");
        stmt.executeUpdate("DROP TABLE IF EXISTS StringCache");
        stmt.executeUpdate("DROP TABLE IF EXISTS PersonCache");

        stmt.executeUpdate("CREATE TABLE LongCache (" +
            " _key BIGINT PRIMARY KEY," +
            " _value BIGINT)");

        stmt.executeUpdate("CREATE TABLE StringCache (" +
            " _key VARCHAR PRIMARY KEY," +
            " _value VARCHAR)");

        stmt.executeUpdate("CREATE TABLE PersonCache (" +
            " companyCode VARCHAR," +
            " departmentCode VARCHAR," +
            " personNum BIGINT," +
            " firstName VARCHAR," +
            " lastName VARCHAR," +
            " fullName VARCHAR," +
            " age SMALLINT," +
            " married BOOLEAN," +
            " height BIGINT," +
            " weight FLOAT," +
            " birthDate DATE)"/*"," +
            " phones VARCHAR ARRAY)"*/);

        conn.commit();

        U.closeQuiet(stmt);

        U.closeQuiet(conn);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    private static final long MAX_MEMORY_SIZE = 10 * 1024 * 1024;

    /**
     * @return Cache configuration for test.
     * @throws Exception In case when failed to create cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() throws Exception {
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setName(CACHE_NAME);
        cc.setCacheMode(PARTITIONED);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setWriteBehindEnabled(false);
        cc.setSqlSchema("cache");

        CacheJdbcPojoStoreFactory<Object, Object> storeFactory = new CacheJdbcPojoStoreFactory<>();
        storeFactory.setDialect(new BasicJdbcDialect());
        storeFactory.setTypes(storeTypes());
        storeFactory.setDataSourceFactory(() -> pg.getPostgresDatabase());

        cc.setCacheStoreFactory(storeFactory);

        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);
        cc.setStoreKeepBinary(false);

        cc.setQueryEntities(Collections.singletonList(new QueryEntity(Long.class.getName(), Long.class.getName()).setTableName("testTable")));

        return cc;
    }

    /**
     * @return Types to be used in test.
     */
    protected JdbcType[] storeTypes() {
        JdbcType[] storeTypes = new JdbcType[3];

        storeTypes[0] = new JdbcType();
        storeTypes[0].setDatabaseSchema("PUBLIC");
        storeTypes[0].setCacheName(CACHE_NAME);
        storeTypes[0].setDatabaseTable("LongCache");
        storeTypes[0].setKeyType("java.lang.Long");
        storeTypes[0].setKeyFields(new JdbcTypeField(Types.BIGINT, "_key", Long.class, null));

        storeTypes[0].setValueType("java.lang.Long");
        storeTypes[0].setValueFields(new JdbcTypeField(Types.BIGINT, "_value", Long.class, null));


        storeTypes[1] = new JdbcType();
        storeTypes[1].setDatabaseSchema("PUBLIC");
        storeTypes[1].setDatabaseTable("StringCache");
        storeTypes[1].setKeyType("java.lang.String");
        storeTypes[1].setCacheName(CACHE_NAME);

        storeTypes[1].setKeyFields(new JdbcTypeField(Types.VARCHAR, "_key", String.class, null));

        storeTypes[1].setValueType("java.lang.String");
        storeTypes[1].setValueFields(new JdbcTypeField(Types.VARCHAR, "_value", String.class, null));

        storeTypes[2] = new JdbcType();
        storeTypes[2].setDatabaseSchema("PUBLIC");
        storeTypes[2].setDatabaseTable("PersonCache");
        storeTypes[2].setKeyType("org.apache.ignite.tests.pojos.PersonId");
        storeTypes[2].setCacheName(CACHE_NAME);

        storeTypes[2].setKeyFields(
            new JdbcTypeField(Types.VARCHAR, "companyCode", String.class, "companyCode"),
            new JdbcTypeField(Types.VARCHAR, "departmentCode", String.class, "departmentCode"),
            new JdbcTypeField(Types.BIGINT, "personNum", Long.class, "personNum"));

        storeTypes[2].setValueType("org.apache.ignite.tests.pojos.Person");
        storeTypes[2].setValueFields(
            new JdbcTypeField(Types.BIGINT, "personNum", Long.class, "personNum"),
            new JdbcTypeField(Types.VARCHAR, "firstName", String.class, "firstName"),
            new JdbcTypeField(Types.VARCHAR, "lastName", String.class, "lastName"),
            new JdbcTypeField(Types.VARCHAR, "fullName", String.class, "fullName"),
            new JdbcTypeField(Types.SMALLINT, "age", Short.class, "age"),
            new JdbcTypeField(Types.BOOLEAN, "married", Boolean.class, "married"),
            new JdbcTypeField(Types.BIGINT, "height", Long.class, "height"),
            new JdbcTypeField(Types.FLOAT, "weight", Float.class, "weight"),
            new JdbcTypeField(Types.DATE, "birthDate", Date.class, "birthDate")/*,
            new JdbcTypeField(Types.ARRAY, "phones", List.class, "phones")*/);

        return storeTypes;
    }

    ListeningTestLogger l = new ListeningTestLogger(false, log());

    LogListener listener;

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration());

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setPageSize(4 * 1024)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(MAX_MEMORY_SIZE)
                .setInitialSize(MAX_MEMORY_SIZE)
                .setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU)
            );

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setMarshaller(null);

        cfg.setGridLogger(l);

        return cfg;
    }

    @Override protected long getTestTimeout() {
        return 600_000;
    }

    /** */
    @Test
    public void testEvictionPolicy() throws Exception{
        startGrid();

        LogListener.Builder builder = LogListener.matches("Page-based evictions started. Consider increasing 'maxSize' on Data Region configuration");

        LogListener listener = builder.atLeast(1).build();

        l.registerListener(listener);

        IgniteCache<Long, Long> cache = grid().getOrCreateCache(CACHE_NAME);

        Map<Long, Long> map = new HashMap<>();

        Long l1 = 0L;

        while (true) {
            log.info(l1);

            cache.put(l1, l1);

            map.put(l1, l1);

           if (listener.check())
               break;

            l1++;
        }

        SqlFieldsQuery query = new SqlFieldsQuery(
            "SELECT * FROM testTable");

        FieldsQueryCursor<List<?>> cursor = cache.query(query);

        Iterator<List<?>> iterator = cursor.iterator();

        Map<Long, Long> m = new HashMap<>();

        while (iterator.hasNext()) {
            List row = iterator.next();

            m.put((Long)row.get(0), (Long)row.get(1));
        }

        List<Long> list = new ArrayList<>(m.keySet());

        list.sort(null);

        assertTrue(m.size() < map.size());

        log.info(list.get(0));

        for (long i = 0; i < list.get(0); i++)
            assertTrue(cache.get(i) != null);
    }

    /** */
    @Test
    public void primitiveStrategyTest() throws Exception{
        Map<Long, Long> longMap = TestsHelper.generateLongsMap();
        Map<String, String> strMap = TestsHelper.generateStringsMap();

        IgniteCache<Object, Object> cache = grid().getOrCreateCache(CACHE_NAME);

        cache.put(1L, 1L);
        cache.put("1", "1");

        cache.putAll(longMap);
        cache.putAll(strMap);

        stopAllGrids();

        startGrid();

        cache = grid().getOrCreateCache(CACHE_NAME);

        assertEquals(longMap.get(1L), cache.get(1L));

        assertEquals(strMap.get("1"), cache.get("1"));

        assertEquals(longMap, cache.getAll(longMap.keySet()));

        assertEquals(strMap, cache.getAll(strMap.keySet()));

        cache.remove(1L);
        cache.removeAll(longMap.keySet());

        cache.remove("1");
        cache.removeAll(strMap.keySet());

        stopAllGrids();

        startGrid();

        cache = grid().getOrCreateCache(CACHE_NAME);

        assertNull(cache.get(1L));

        assertNull(cache.get("1"));

        assertEquals(0, cache.getAll(longMap.keySet()).size());

        assertEquals(0, cache.getAll(strMap.keySet()).size());
    }

    /** */
    @Test
    public void pojoStrategyTest() throws Exception {
        Map<PersonId, Person> personMap = TestsHelper.generatePersonIdsPersonsMap();

        IgniteCache<PersonId, Person> cache = grid().getOrCreateCache(CACHE_NAME);

        PersonId id = TestsHelper.generateRandomPersonId();

        Person person = TestsHelper.generateRandomPerson(id.getPersonNumber());

        cache.put(id, person);

        cache.putAll(personMap);

        stopAllGrids();

        startGrid();

        cache = grid().getOrCreateCache(CACHE_NAME);

        assertTrue(cache.get(id).equalsPrimitiveFields(person));

        assertEquals(personMap ,cache.getAll(personMap.keySet()));

        cache.remove(id);

        cache.removeAll(personMap.keySet());

        stopAllGrids();

        startGrid();

        cache = grid().getOrCreateCache(CACHE_NAME);

        assertNull(cache.get(id));

        assertEquals(0 ,cache.getAll(personMap.keySet()).size());
    }

    public CacheJdbcPojoStore cacheStore() throws Exception {
        CacheJdbcPojoStore cacheStore = (CacheJdbcPojoStore)cacheConfiguration()
            .getCacheStoreFactory()
            .create();

        inject(cacheStore);

        return cacheStore;
    }

    /** */
    @SuppressWarnings("unchecked")
    private void pojoStrategyTransactionTest(Ignite ignite, TransactionConcurrency concurrency,
        TransactionIsolation isolation) throws Exception {
        CacheJdbcPojoStore cacheStore = cacheStore();

        IgniteCache<Object, Object> cache = grid().getOrCreateCache(CACHE_NAME);

        IgniteCacheObjectProcessor proc = grid().context().cacheObjects();

        Map<PersonId, Person> personMap = TestsHelper.generatePersonIdsPersonsMap();

        Map<Object, Object> binaryPersonMap = new HashMap<>();

        personMap.forEach((key, value) -> binaryPersonMap.put(
            proc.marshalToBinary(key, false),
            proc.marshalToBinary(value, false)));

        PersonId id = TestsHelper.generateRandomPersonId();

        Object binaryId = proc.marshalToBinary(id, false);

        Person person = TestsHelper.generateRandomPerson(id.getPersonNumber());

        Object binaryPerson = proc.marshalToBinary(person, false);

        IgniteTransactions txs = ignite.transactions();

        Transaction tx = txs.txStart(concurrency, isolation);

        try {
            cache.put(id, person);

            assertNull(cacheStore.load(binaryId));

            assertEquals(0, cacheStore.loadAll(binaryPersonMap.keySet()).size());

            tx.commit();
        }
        finally {
            U.closeQuiet(tx);
        }

        Object loadedPerson = cacheStore.load(binaryId);

        assertNotNull(loadedPerson);

        assertEquals(binaryPerson, loadedPerson);

        tx = txs.txStart(concurrency, isolation);

        try {
            cache.putAll(personMap);

            assertEquals(0, cacheStore.loadAll(binaryPersonMap.keySet()).size());

            tx.commit();
        }
        finally {
            U.closeQuiet(tx);
        }

        Map<PersonId, Person> loadedPersonMap = cacheStore.loadAll(binaryPersonMap.keySet());

        assertNotNull(loadedPerson);

        assertEquals(binaryPersonMap, loadedPersonMap);

        tx = txs.txStart(concurrency, isolation);

        try {
            cache.remove(id);

            assertNotNull(cacheStore.load(binaryId));

            tx.commit();
        }
        finally {
            U.closeQuiet(tx);
        }

        assertNull(cacheStore.load(binaryId));

        tx = txs.txStart(concurrency, isolation);

        try {
            cache.removeAll(binaryPersonMap.keySet());

            assertEquals(personMap.size(), cacheStore.loadAll(binaryPersonMap.keySet()).size());

            tx.commit();
        }
        finally {
            U.closeQuiet(tx);
        }

        assertEquals(0, cacheStore.loadAll(binaryPersonMap.keySet()).size());
    }

    /** */
    @Test
    public void pojoStrategyTransactionTest() throws Exception {
        pojoStrategyTransactionTest(grid(), TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
        pojoStrategyTransactionTest(grid(), TransactionConcurrency.OPTIMISTIC, TransactionIsolation.REPEATABLE_READ);
        pojoStrategyTransactionTest(grid(), TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);
        pojoStrategyTransactionTest(grid(), TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);
        pojoStrategyTransactionTest(grid(), TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ);
        pojoStrategyTransactionTest(grid(), TransactionConcurrency.PESSIMISTIC, TransactionIsolation.SERIALIZABLE);
    }
}
