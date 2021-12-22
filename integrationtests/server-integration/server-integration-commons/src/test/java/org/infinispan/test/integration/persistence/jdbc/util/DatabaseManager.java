package org.infinispan.test.integration.persistence.jdbc.util;

import java.io.IOException;
import java.util.Properties;

import org.infinispan.persistence.jdbc.common.DatabaseType;
import org.infinispan.persistence.jdbc.common.configuration.PooledConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;

public class DatabaseManager {

    private static Properties props;

    static {
        loadDBProperties();
    }

    public static TableManipulationConfigurationBuilder buildTableManipulation(JdbcStringBasedStoreConfigurationBuilder storeBuilder) {
        //workaround for sybase160 https://issues.redhat.com/browse/JDG-1492
        storeBuilder.addProperty("infinispan.jdbc.upsert.disabled", props.getProperty("database.upsert.disabled"));
        return storeBuilder.table()
                .tableNamePrefix("TEST_TABLE")
                .idColumnName("ID_COLUMN").idColumnType(props.getProperty("id.column.type"))
                .dataColumnName("DATA_COLUMN").dataColumnType(props.getProperty("data.column.type"))
                .timestampColumnName("TIMESTAMP_COLUMN").timestampColumnType(props.getProperty("timestamp.column.type"))
                .segmentColumnName("SEGMENT_COLUMN").segmentColumnType("INTEGER");
    }

    public static PooledConnectionFactoryConfiguration configureUniqueConnectionFactory(JdbcStringBasedStoreConfigurationBuilder storeBuilder) {
        return storeBuilder.connectionPool()
                .driverClass(props.getProperty("driver.class"))
                .connectionUrl(props.getProperty("connection.url"))
                .username(props.getProperty("db.username"))
                .password(props.getProperty("db.password"))
                .driverClass(props.getProperty("driver.class"))
                .create();
    }

    public static JdbcStringBasedStoreConfigurationBuilder createConfigurationBuilder() {
        return TestCacheManagerFactory
                .getDefaultCacheConfiguration(false)
                .persistence()
                .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
                .dialect(DatabaseType.valueOf(props.getProperty("database.type")));
    }

    public static Properties loadDBProperties() {
        props = new Properties();
        try {
            props.load(DatabaseManager.class.getResourceAsStream("/connection.properties"));
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to read DB properties");
        }
        return props;
    }
}
