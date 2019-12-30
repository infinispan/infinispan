package org.infinispan.server.persistence;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionType;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.PooledConnectionFactoryConfigurationBuilder;
import org.infinispan.server.test.junit4.DatabaseServerRule;
import org.infinispan.util.concurrent.IsolationLevel;

public class JdbcConfigurationUtil {

    private PooledConnectionFactoryConfigurationBuilder persistenceConfiguration;
    private ConfigurationBuilder configurationBuilder;
    private CacheMode cacheMode;

    public JdbcConfigurationUtil(CacheMode cacheMode) {
        configurationBuilder = new ConfigurationBuilder();
        this.cacheMode = cacheMode;
    }

    public JdbcConfigurationUtil createPersistenceConfiguration(DatabaseServerRule database, boolean passivation) {
        persistenceConfiguration = configurationBuilder.clustering().cacheMode(cacheMode).hash().numOwners(1)
              .persistence()
              .passivation(passivation)
              .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
              .purgeOnStartup(false)
              .shared(false)
              .preload(true)
              .fetchPersistentState(false)
              .table()
              .dropOnExit(true)
              .createOnStart(true)
              .tableNamePrefix("TBL")
              .idColumnName("ID").idColumnType(database.getDatabase().getIdColumType())
              .dataColumnName("DATA").dataColumnType(database.getDatabase().getDataColumnType())
              .timestampColumnName("TS").timestampColumnType(database.getDatabase().getTimeStampColumnType())
              .segmentColumnName("S").segmentColumnType(database.getDatabase().getSegmentColumnType())
              .connectionPool()
              .connectionUrl(database.getDatabase().jdbcUrl())
              .username(database.getDatabase().username())
              .password(database.getDatabase().password())
              .driverClass(database.getDatabase().driverClassName());
        return this;
    }

    public JdbcConfigurationUtil setLockingConfigurations() {
        configurationBuilder.locking().isolationLevel(IsolationLevel.READ_COMMITTED).lockAcquisitionTimeout(20000).concurrencyLevel(500).useLockStriping(false);
        return this;
    }

    public JdbcConfigurationUtil setEvition() {
        configurationBuilder.memory().size(2).evictionType(EvictionType.MEMORY);
        return this;
    }

    public PooledConnectionFactoryConfigurationBuilder getPersistenceConfiguration() {
        return this.persistenceConfiguration;
    }

    public ConfigurationBuilder getConfigurationBuilder() {
        return configurationBuilder;
    }

}
