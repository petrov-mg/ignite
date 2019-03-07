package org.apache.ignite.cache.store.jdbc;

import com.mchange.v2.c3p0.DriverManagerDataSource;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.UUID;
import org.apache.ignite.cache.store.jdbc.dialect.BasicJdbcDialect;

/** */
public class PostgresCacheJdbcPojoStoreTest extends CacheJdbcPojoStoreTest {
    /** Default conn url. */
    private static final String DFLT_CONN_URL = "jdbc:postgresql://localhost:8090/postgres";

    /** */
    public PostgresCacheJdbcPojoStoreTest() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override protected void performStatement(Statement stmt) throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS String_Entries");

        stmt.executeUpdate("DROP TABLE IF EXISTS UUID_Entries");

        stmt.executeUpdate("DROP TABLE IF EXISTS Organization");

        stmt.executeUpdate("DROP TABLE IF EXISTS Person");

        stmt.executeUpdate("DROP TABLE IF EXISTS Timestamp_Entries");

        stmt.executeUpdate("DROP TABLE IF EXISTS Binary_Entries");

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " +
            "String_Entries (key varchar(100) not null, val varchar(100), PRIMARY KEY(key))");

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " +
            "UUID_Entries (key bytea not null, val bytea, PRIMARY KEY(key))");

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " +
            "Binary_Entries (key integer not null, val bytea, PRIMARY KEY(key))");

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " +
            "Timestamp_Entries (key timestamp not null, val integer, PRIMARY KEY(key))");

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " +
            "Organization (id integer not null, name varchar(50), city varchar(50), PRIMARY KEY(id))");

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " +
            "Person (id integer not null, org_id integer, name varchar(50), PRIMARY KEY(id))");

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " +
            "Person_Complex (id integer not null, org_id integer not null, city_id integer not null, " +
            "name varchar(50), salary integer, PRIMARY KEY(id, org_id, city_id))");
    }

    /** {@inheritDoc} */
    @Override protected CacheJdbcPojoStore<Object, Object> store() {
        CacheJdbcPojoStoreFactory<Object, Object> storeFactory = new CacheJdbcPojoStoreFactory<>();

        JdbcType[] storeTypes = new JdbcType[7];

        storeTypes[0] = new JdbcType();
        storeTypes[0].setDatabaseSchema("PUBLIC");
        storeTypes[0].setDatabaseTable("ORGANIZATION");
        storeTypes[0].setKeyType("org.apache.ignite.cache.store.jdbc.model.OrganizationKey");
        storeTypes[0].setKeyFields(new JdbcTypeField(Types.INTEGER, "ID", Integer.class, "id"));

        storeTypes[0].setValueType("org.apache.ignite.cache.store.jdbc.model.Organization");
        storeTypes[0].setValueFields(
            new JdbcTypeField(Types.INTEGER, "ID", Integer.class, "id"),
            new JdbcTypeField(Types.VARCHAR, "NAME", String.class, "name"),
            new JdbcTypeField(Types.VARCHAR, "CITY", String.class, "city"));

        storeTypes[1] = new JdbcType();
        storeTypes[1].setDatabaseSchema("PUBLIC");
        storeTypes[1].setDatabaseTable("PERSON");
        storeTypes[1].setKeyType("org.apache.ignite.cache.store.jdbc.model.PersonKey");
        storeTypes[1].setKeyFields(new JdbcTypeField(Types.INTEGER, "ID", Integer.class, "id"));

        storeTypes[1].setValueType("org.apache.ignite.cache.store.jdbc.model.Person");
        storeTypes[1].setValueFields(
            new JdbcTypeField(Types.INTEGER, "ID", Integer.class, "id"),
            new JdbcTypeField(Types.INTEGER, "ORG_ID", Integer.class, "orgId"),
            new JdbcTypeField(Types.VARCHAR, "NAME", String.class, "name"));

        storeTypes[2] = new JdbcType();
        storeTypes[2].setDatabaseSchema("PUBLIC");
        storeTypes[2].setDatabaseTable("PERSON_COMPLEX");
        storeTypes[2].setKeyType("org.apache.ignite.cache.store.jdbc.model.PersonComplexKey");
        storeTypes[2].setKeyFields(
            new JdbcTypeField(Types.INTEGER, "ID", int.class, "id"),
            new JdbcTypeField(Types.INTEGER, "ORG_ID", int.class, "orgId"),
            new JdbcTypeField(Types.INTEGER, "CITY_ID", int.class, "cityId"));

        storeTypes[2].setValueType("org.apache.ignite.cache.store.jdbc.model.Person");
        storeTypes[2].setValueFields(
            new JdbcTypeField(Types.INTEGER, "ID", Integer.class, "id"),
            new JdbcTypeField(Types.INTEGER, "ORG_ID", Integer.class, "orgId"),
            new JdbcTypeField(Types.VARCHAR, "NAME", String.class, "name"),
            new JdbcTypeField(Types.INTEGER, "SALARY", Integer.class, "salary"));

        storeTypes[3] = new JdbcType();
        storeTypes[3].setDatabaseSchema("PUBLIC");
        storeTypes[3].setDatabaseTable("TIMESTAMP_ENTRIES");
        storeTypes[3].setKeyType("java.sql.Timestamp");
        storeTypes[3].setKeyFields(new JdbcTypeField(Types.TIMESTAMP, "KEY", Timestamp.class, null));

        storeTypes[3].setValueType("java.lang.Integer");
        storeTypes[3].setValueFields(new JdbcTypeField(Types.INTEGER, "VAL", Integer.class, null));

        storeTypes[4] = new JdbcType();
        storeTypes[4].setDatabaseSchema("PUBLIC");
        storeTypes[4].setDatabaseTable("STRING_ENTRIES");
        storeTypes[4].setKeyType("java.lang.String");
        storeTypes[4].setKeyFields(new JdbcTypeField(Types.VARCHAR, "KEY", String.class, null));

        storeTypes[4].setValueType("java.lang.String");
        storeTypes[4].setValueFields(new JdbcTypeField(Types.VARCHAR, "VAL", Integer.class, null));

        storeTypes[5] = new JdbcType();
        storeTypes[5].setDatabaseSchema("PUBLIC");
        storeTypes[5].setDatabaseTable("UUID_ENTRIES");
        storeTypes[5].setKeyType("java.util.UUID");
        storeTypes[5].setKeyFields(new JdbcTypeField(Types.BINARY, "KEY", UUID.class, null));

        storeTypes[5].setValueType("java.util.UUID");
        storeTypes[5].setValueFields(new JdbcTypeField(Types.BINARY, "VAL", UUID.class, null));

        storeTypes[6] = new JdbcType();
        storeTypes[6].setDatabaseSchema("PUBLIC");
        storeTypes[6].setDatabaseTable("BINARY_ENTRIES");
        storeTypes[6].setKeyType("org.apache.ignite.cache.store.jdbc.model.BinaryTestKey");
        storeTypes[6].setKeyFields(new JdbcTypeField(Types.INTEGER, "KEY", Integer.class, "id"));

        storeTypes[6].setValueType("org.apache.ignite.cache.store.jdbc.model.BinaryTest");
        storeTypes[6].setValueFields(new JdbcTypeField(Types.BINARY, "VAL", byte[].class, "bytes"));

        storeFactory.setTypes(storeTypes);

        storeFactory.setDialect(new BasicJdbcDialect());

        CacheJdbcPojoStore<Object, Object> store = storeFactory.create();

        DriverManagerDataSource dataSrc = new DriverManagerDataSource();

        dataSrc.setJdbcUrl(DFLT_CONN_URL);
        dataSrc.setUser("postgres");

        store.setDataSource(dataSrc);

        return store;
    }
}
