package org.apache.ignite.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.client.Config;

/** */
public class JdbcClientDdlAuthorizationTest extends AbstractDdlSqlAuthorizationTest {
    /** */
    public static final String JDBC_URL_PREFIX = "jdbc:ignite:thin://";

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

