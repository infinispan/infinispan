package org.infinispan.test.integration.persistence.jdbc.util;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.persistence.jdbc.common.configuration.PooledConnectionFactoryConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;

import java.io.IOException;
import java.util.Properties;

public class JdbcConfigurationUtil {

    private PooledConnectionFactoryConfigurationBuilder persistenceConfiguration;
    private ConfigurationBuilder configurationBuilder;
    public static final String CACHE_NAME = "jdbc";

    public JdbcConfigurationUtil(boolean passivation, boolean preload) {
        configurationBuilder = new ConfigurationBuilder();
        createPersistenceConfiguration(passivation, preload);
    }

    private JdbcConfigurationUtil createPersistenceConfiguration(boolean passivation, boolean preload) {
        Properties props = loadDBProperties();
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
                .driverClass(props.getProperty("driver.class"))
                .connectionUrl(props.getProperty("connection.url"))
                .username(props.getProperty("db.username"))
                .password(props.getProperty("db.password"));
        persistenceConfiguration.addProperty("infinispan.jdbc.upsert.disabled", props.getProperty("database.upsert.disabled"));
        return this;
    }

    public PooledConnectionFactoryConfigurationBuilder getPersistenceConfiguration() {
        return this.persistenceConfiguration;
    }

    public ConfigurationBuilder getConfigurationBuilder() {
        return configurationBuilder;
    }

    public DefaultCacheManager getCacheManager() {
        Configuration configuration = configurationBuilder.build();
        GlobalConfigurationBuilder configurationBuilder = new GlobalConfigurationBuilder().nonClusteredDefault().defaultCacheName(CACHE_NAME);
        DefaultCacheManager manager = new DefaultCacheManager(configurationBuilder.build(), configuration);
        return manager;
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
