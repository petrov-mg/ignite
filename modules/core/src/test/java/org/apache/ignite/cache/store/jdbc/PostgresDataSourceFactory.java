package org.apache.ignite.cache.store.jdbc;

import com.mchange.v2.c3p0.DriverManagerDataSource;
import javax.cache.configuration.Factory;
import javax.sql.DataSource;

/** */
public class  PostgresDataSourceFactory implements Factory<DataSource> {
    /** DB connection URL. */
    private static final String DFLT_CONN_URL = "jdbc:postgresql://localhost:8090/postgres";

    /** {@inheritDoc} */
    @Override public DataSource create() {
        DriverManagerDataSource dataSrc = new DriverManagerDataSource();

        dataSrc.setJdbcUrl(DFLT_CONN_URL);
        dataSrc.setUser("postgres");

        return dataSrc;
    }

}
