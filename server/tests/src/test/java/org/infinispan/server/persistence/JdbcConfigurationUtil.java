package org.infinispan.server.persistence;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.configuration.PooledConnectionFactoryConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.server.test.core.persistence.Database;
import org.infinispan.util.concurrent.IsolationLevel;

public class JdbcConfigurationUtil {

    private PooledConnectionFactoryConfigurationBuilder persistenceConfiguration;
    private ConfigurationBuilder configurationBuilder;
    private CacheMode cacheMode;
    public String driverClass;

    public JdbcConfigurationUtil(CacheMode cacheMode, Database database, boolean passivation, boolean preload) {
        configurationBuilder = new ConfigurationBuilder();
        this.cacheMode = cacheMode;
        createPersistenceConfiguration(database, passivation, preload);
    }

    private JdbcConfigurationUtil createPersistenceConfiguration(Database database, boolean passivation, boolean preload) {
        driverClass = database.driverClassName();
        persistenceConfiguration = configurationBuilder.clustering().cacheMode(cacheMode).hash().numOwners(1)
                .persistence()
                .passivation(passivation)
                .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
                .shared(false)
                .preload(preload)
                .fetchPersistentState(true)
                .table()
                .dropOnExit(false)
                .createOnStart(true)
                .tableNamePrefix("tbl")
                .idColumnName("id").idColumnType(database.getIdColumType())
                .dataColumnName("data").dataColumnType(database.getDataColumnType())
                .timestampColumnName("ts").timestampColumnType(database.getTimeStampColumnType())
                .segmentColumnName("s").segmentColumnType(database.getSegmentColumnType())
                .connectionPool()
                .connectionUrl(database.jdbcUrl())
                .username(database.username())
                .password(database.password())
                .driverClass(database.driverClassName());
        return this;
    }

    public JdbcConfigurationUtil setLockingConfigurations() {
        configurationBuilder.locking().isolationLevel(IsolationLevel.READ_COMMITTED).lockAcquisitionTimeout(20000).concurrencyLevel(500).useLockStriping(false);
        return this;
    }

    public JdbcConfigurationUtil setEvition() {
        configurationBuilder.memory().maxCount(2);
        return this;
    }

    public PooledConnectionFactoryConfigurationBuilder getPersistenceConfiguration() {
        return this.persistenceConfiguration;
    }

    public ConfigurationBuilder getConfigurationBuilder() {
        return configurationBuilder;
    }

}
