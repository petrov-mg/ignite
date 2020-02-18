package org.apache.ignite.jdbc;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_DESTROY;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;

/** */
public abstract class AbstractDdlSqlAuthorizationTest extends AbstractSqlAuthorizationTest {
    /** */
    protected static final String TEST_TABLE = "test_table";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(getConfiguration(0,
            systemPermissionsHolder(CACHE_CREATE),
            systemPermissionsHolder(CACHE_DESTROY),
            emptyPermissionsHolder())
        );

        startGrid(getConfiguration(1)).cluster().state(ACTIVE);

        executeWithSystemPermission(
            "CREATE TABLE " + TEST_TABLE + "(id LONG PRIMARY KEY, str_col varchar, long_col LONG)" +
                " WITH \"TEMPLATE=REPLICATED\";",
            CACHE_CREATE
        );
    }

    /** */
    protected IgniteConfiguration getConfiguration(int idx, TestSecurityData... users) throws Exception {
        String instanceName = getTestIgniteInstanceName(idx);

        return getConfiguration(
            instanceName,
            new TestSecurityPluginProvider(
                instanceName,
                null,
                systemPermissions(JOIN_AS_SERVER),
                false,
                users
            )
        );
    }

    /** */
    @Test
    public void testCreateTable() throws Exception {
        checkSystemPermissionRequired(
            "CREATE TABLE test_create_table(id LONG PRIMARY KEY, val varchar) WITH \"TEMPLATE=REPLICATED\";",
            CACHE_CREATE
        );
    }

    /** */
    @Test
    public void testDropTable() throws Exception {
        String testDropTable = "test_drop_table";

        executeWithSystemPermission(
            "CREATE TABLE " + testDropTable + "(id LONG PRIMARY KEY, str_col varchar, long_col LONG)" +
                " WITH" + " \"TEMPLATE=REPLICATED\";",
            CACHE_CREATE
        );

        checkSystemPermissionRequired("DROP TABLE " + testDropTable + ';', CACHE_DESTROY);
    }

    /** */
    @Test
    public void testAlterTableAddColumn() throws Exception {
        checkNoPermissionsRequired("ALTER TABLE " + TEST_TABLE + " ADD test_add_column LONG;");
    }

    /** */
    @Test
    public void testAlterTableDropColumn() throws Exception {
        checkNoPermissionsRequired("ALTER TABLE " + TEST_TABLE + " DROP COLUMN long_col;");
    }

    /** */
    @Test
    public void testCreateIndex() throws Exception {
        checkNoPermissionsRequired("CREATE INDEX test_create_idx ON " + TEST_TABLE + "(id ASC);");
    }

    /** */
    @Test
    public void testDropIndex() throws Exception {
        execute(NO_PERMISSIONS_USER, "CREATE INDEX test_drop_idx ON " + TEST_TABLE + "(id ASC);");

        checkNoPermissionsRequired("DROP INDEX test_drop_idx ON " + TEST_TABLE + ';');
    }
}
