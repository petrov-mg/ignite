package org.apache.ignite.jdbc;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;

/** */
public abstract class AbstractDmlSqlAuthorizationTest extends AbstractSqlAuthorizationTest {
    /** */
    private static final String TEST_CACHE = "test_cache";

    /** */
    private static final String TEST_SCHEMA = "test_schema";

    /** */
    private static final String TABLE_NAME = Integer.class.getSimpleName();

    /** */
    private static final int INSERT_KEY_IDX = 1;

    /** */
    private static final int SELECT_KEY_IDX = 2;

    /** */
    private static final int UPDATE_KEY_IDX = 3;

    /** */
    private static final int DELETE_KEY_IDX = 4;

    /** */
    private static final int MERGE_KEY_IDX = 5;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(getConfiguration(0,
            cachePermissionsHolder(TEST_CACHE, CACHE_READ),
            cachePermissionsHolder(TEST_CACHE, CACHE_PUT),
            cachePermissionsHolder(TEST_CACHE, CACHE_REMOVE),
            emptyPermissionsHolder())
        );

        startGrid(getConfiguration(1)).cluster().state(ACTIVE);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(TEST_CACHE);

        ccfg.setIndexedTypes(Integer.class, Integer.class);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setSqlSchema(TEST_SCHEMA);

        grid(0).createCache(ccfg);
    }

    /** */
    private IgniteConfiguration getConfiguration(int idx, TestSecurityData... users) throws Exception {
        String instanceName = getTestIgniteInstanceName(idx);

        return getConfiguration(
            instanceName,
            new TestSecurityPluginProvider(
                instanceName,
                null,
                systemPermissions(CACHE_CREATE, JOIN_AS_SERVER),
                false,
                users
            )
        );
    }

    /** */
    @Test
    public void testInsertLocal() throws Exception {
        doInsert(0);
    }

    /** */
    @Test
    public void testInsertRemote() throws Exception {
        doInsert(1);
    }

    /** */
    @Test
    public void testSelectLocal() throws Exception {
        doSelect(0);
    }

    /** */
    @Test
    public void testSelectRemote() throws Exception {
        doSelect(1);
    }

    /** */
    @Test
    public void testUpdateLocal() throws Exception {
        doUpdate(0);
    }

    /** */
    @Test
    public void testUpdateRemote() throws Exception {
        doUpdate(1);
    }

    /** */
    @Test
    public void testDeleteLocal() throws Exception {
        doDelete(0);
    }

    /** */
    @Test
    public void testDeleteRemote() throws Exception {
        doDelete(1);
    }

    /** */
    @Test
    public void testMergeLocal() throws Exception {
        doMerge(0);
    }

    /** */
    @Test
    public void testMergeRemote() throws Exception {
        doMerge(1);
    }

    /** */
    private void doInsert(int nodeIdx) throws Exception {
        int key = keyForNode(INSERT_KEY_IDX, nodeIdx);

        checkCachePermissionsRequired(
            "INSERT INTO " + TEST_SCHEMA + '.' + TABLE_NAME + "(_key, _val) VALUES (" + key + ", 0)",
            TEST_CACHE, CACHE_PUT
        );
    }

    /** */
    private void doSelect(int nodeIdx) throws Exception {
        int key = keyForNode(SELECT_KEY_IDX, nodeIdx);

        executeWithCachePermission(
            "INSERT INTO " + TEST_SCHEMA + '.' + TABLE_NAME + "(_key, _val) VALUES (" + key + ", 0)",
            TEST_CACHE, CACHE_PUT
        );

        checkCachePermissionsRequired(
            "SELECT _val FROM " + TEST_SCHEMA + '.' + TABLE_NAME + " WHERE _key=" + key + ';',
            TEST_CACHE, CACHE_READ
        );
    }

    /** */
    private void doUpdate(int nodeIdx) throws Exception {
        int key = keyForNode(UPDATE_KEY_IDX, nodeIdx);

        executeWithCachePermission(
            "INSERT INTO " + TEST_SCHEMA + '.' + TABLE_NAME + "(_key, _val) VALUES (" + key + ", 0)",
            TEST_CACHE, CACHE_PUT
        );

        checkCachePermissionsRequired(
            "UPDATE " + TEST_SCHEMA + '.' + TABLE_NAME + " SET _val = 1 WHERE _key=" + key + ';',
            TEST_CACHE, CACHE_PUT
        );
    }

    /** */
    private void doDelete(int nodeIdx) throws Exception {
        int key = keyForNode(DELETE_KEY_IDX, nodeIdx);

        executeWithCachePermission(
            "INSERT INTO " + TEST_SCHEMA + '.' + TABLE_NAME + "(_key, _val) VALUES (" + key + ", 0)",
            TEST_CACHE, CACHE_PUT
        );

        checkCachePermissionsRequired(
            "DELETE FROM " + TEST_SCHEMA + '.' + TABLE_NAME + " WHERE _key=" + key + ';',
            TEST_CACHE, CACHE_REMOVE
        );
    }

    /** */
    private void doMerge(int nodeIdx) throws Exception {
        int key = keyForNode(MERGE_KEY_IDX, nodeIdx);

        checkCachePermissionsRequired(
            "MERGE INTO " + TEST_SCHEMA + '.' + TABLE_NAME + "(_key, _val) VALUES (" + key + ", 0);",
            TEST_CACHE, CACHE_PUT
        );
    }

    /** */
    private int keyForNode(int keyIdx, int nodeIdx) {
        int res = 0;

        AtomicInteger cnt = new AtomicInteger(0);

        for (int i = 0; i < keyIdx; i++) {
            res = keyForNode(
                grid(0).affinity(TEST_CACHE),
                cnt,
                grid(nodeIdx).localNode()
            );
        }

        return res;
    }
}
