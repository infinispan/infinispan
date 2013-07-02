package org.infinispan.loaders.jdbc.configuration;

import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.LegacyLoaderAdapter;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStoreConfig;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.util.TypedProperties;

@BuiltBy(JdbcBinaryCacheStoreConfigurationBuilder.class)
public class JdbcBinaryCacheStoreConfiguration extends AbstractJdbcCacheStoreConfiguration implements LegacyLoaderAdapter<JdbcBinaryCacheStoreConfig> {

   private final TableManipulationConfiguration table;

   JdbcBinaryCacheStoreConfiguration(TableManipulationConfiguration table, ConnectionFactoryConfiguration connectionFactory, long lockAcquistionTimeout, int lockConcurrencyLevel,
         boolean purgeOnStartup, boolean purgeSynchronously, int purgerThreads, boolean fetchPersistentState, boolean ignoreModifications, TypedProperties properties,
         AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(connectionFactory, lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications,
            properties, async, singletonStore);
      this.table = table;
   }

   public TableManipulationConfiguration table() {
      return table;
   }

   @Override
   public JdbcBinaryCacheStoreConfig adapt() {
      JdbcBinaryCacheStoreConfig config = new JdbcBinaryCacheStoreConfig();
      // StoreConfiguration
      LegacyConfigurationAdaptor.adapt(this, config);

      // ConnectionFactory
      ((LegacyConnectionFactoryAdaptor) connectionFactory()).adapt(config);

      // TableManipulation
      config.setCreateTableOnStart(table.createOnStart());
      config.setDropTableOnExit(table.dropOnExit());
      config.setBatchSize(table.batchSize());
      config.setFetchSize(table.fetchSize());
      config.setDatabaseType(table.databaseType());
      config.setCacheName(table.cacheName());
      config.setDataColumnName(table.dataColumnName());
      config.setDataColumnType(table.dataColumnType());
      config.setIdColumnName(table.idColumnName());
      config.setIdColumnType(table.idColumnType());
      config.setTimestampColumnName(table.timestampColumnName());
      config.setTimestampColumnType(table.timestampColumnType());
      config.setTableNamePrefix(table.tableNamePrefix());

      return config;
   }

   @Override
   public String toString() {
      return "JdbcBinaryCacheStoreConfiguration [table=" + table + ", connectionFactory()=" + connectionFactory() + ", lockAcquistionTimeout()=" + lockAcquistionTimeout()
            + ", lockConcurrencyLevel()=" + lockConcurrencyLevel() + ", async()=" + async() + ", singletonStore()=" + singletonStore() + ", purgeOnStartup()=" + purgeOnStartup()
            + ", purgeSynchronously()=" + purgeSynchronously() + ", purgerThreads()=" + purgerThreads() + ", fetchPersistentState()=" + fetchPersistentState()
            + ", ignoreModifications()=" + ignoreModifications() + ", properties()=" + properties() + "]";
   }
}
