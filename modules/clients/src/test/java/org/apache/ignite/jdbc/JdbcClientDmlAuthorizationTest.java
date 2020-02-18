package org.apache.ignite.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.client.Config;
import org.junit.Ignore;

/** */
public class JdbcClientDmlAuthorizationTest extends AbstractDmlSqlAuthorizationTest {
    /** */
    public static final String JDBC_URL_PREFIX = "jdbc:ignite:thin://";

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-12589")
    @Override public void testInsertRemote() throws Exception {
       // No-op.
    }

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-12589")
    @Override public void testUpdateRemote() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-12589")
    @Override public void testMergeRemote() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-12589")
    @Override public void testDeleteRemote() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-12589")
    @Override public void testSelectRemote() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void execute(String login, String sql) throws Exception {
        try (Connection conn = getConnection(login)) {
            Statement stmt = conn.createStatement();

            stmt.execute(sql);
        }
    }

    /** */
    protected Connection getConnection(String login) throws SQLException {
        return DriverManager.getConnection(JDBC_URL_PREFIX + Config.SERVER, login, "");
    }
}
