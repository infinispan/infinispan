package org.infinispan.test.integration.persistence.jdbc.util;

import java.io.IOException;
import java.util.Properties;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.persistence.jdbc.common.configuration.PooledConnectionFactoryConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;

public class JdbcConfigurationUtil {

    private PooledConnectionFactoryConfigurationBuilder<?> persistenceConfiguration;
    private final ConfigurationBuilder configurationBuilder;
    public static final String CACHE_NAME = "jdbc";
    public String driverClass;

    public JdbcConfigurationUtil(boolean passivation, boolean preload) {
        configurationBuilder = new ConfigurationBuilder();
        createPersistenceConfiguration(passivation, preload);
    }

    private void createPersistenceConfiguration(boolean passivation, boolean preload) {
        Properties props = loadDBProperties();
        driverClass = props.getProperty("driver.class");
        configurationBuilder.memory().maxCount(2);
        persistenceConfiguration = configurationBuilder
                .persistence()
                .passivation(passivation)
                .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
                .segmented(false)
                .preload(preload)
                .table()
                .createOnStart(true)
                .tableNamePrefix("tbl")
                .idColumnName("ID_COLUMN").idColumnType(props.getProperty("id.column.type"))
                .dataColumnName("DATA_COLUMN").dataColumnType(props.getProperty("data.column.type"))
                .timestampColumnName("TIMESTAMP_COLUMN").timestampColumnType(props.getProperty("timestamp.column.type"))
                .connectionPool()
                .driverClass(driverClass)
                .connectionUrl(props.getProperty("connection.url"))
                .username(props.getProperty("db.username"))
                .password(props.getProperty("db.password"));
        persistenceConfiguration.addProperty("infinispan.jdbc.upsert.disabled", props.getProperty("database.upsert.disabled"));
    }

    public PooledConnectionFactoryConfigurationBuilder<?> getPersistenceConfiguration() {
        return this.persistenceConfiguration;
    }

    public DefaultCacheManager getCacheManager() {
        ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
        holder.global().nonClusteredDefault().defaultCacheName(CACHE_NAME);
        holder.newConfigurationBuilder(CACHE_NAME);
        return new DefaultCacheManager(holder);
    }

    private static Properties loadDBProperties() {
        Properties props = new Properties();
        try {
            props.load(JdbcConfigurationUtil.class.getResourceAsStream("/connection.properties"));
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to read DB properties");
        }
        return props;
    }

}
