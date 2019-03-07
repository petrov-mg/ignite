package org.apache.ignite.cache.store.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.ignite.cache.store.jdbc.dialect.BasicJdbcDialect;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.marshaller.Marshaller;

/** */
public class PostgresCacheJdbcPojoStoreSelfTest extends CacheJdbcPojoStoreAbstractSelfTest {
    /** Default conn url. */
    private static final String DFLT_CONN_URL = "jdbc:postgresql://localhost:8090/postgres";

    /** {@inheritDoc} */
    @Override protected Marshaller marshaller() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected Connection getConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(DFLT_CONN_URL, "postgres", "");

        conn.setAutoCommit(false);

        return conn;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() throws Exception {
        CacheConfiguration cc = super.cacheConfiguration();

        CacheJdbcPojoStoreFactory<Object, Object> storeFactory = new CacheJdbcPojoStoreFactory<>();
        storeFactory.setDialect(new BasicJdbcDialect());
        storeFactory.setTypes(storeTypes());
        storeFactory.setDataSourceFactory(new PostgresDataSourceFactory());
        storeFactory.setSqlEscapeAll(sqlEscapeAll());
        storeFactory.setParallelLoadCacheMinimumThreshold(parallelLoadThreshold);

        cc.setCacheStoreFactory(storeFactory);

        return cc;
    }
}
