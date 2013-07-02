package org.infinispan.loaders.jdbc.configuration;

import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.cache.LegacyLoaderAdapter;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStoreConfig;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.util.TypedProperties;

@BuiltBy(JdbcStringBasedCacheStoreConfigurationBuilder.class)
public class JdbcStringBasedCacheStoreConfiguration extends AbstractJdbcCacheStoreConfiguration implements LegacyLoaderAdapter<JdbcStringBasedCacheStoreConfig> {

   private final String key2StringMapper;
   private final TableManipulationConfiguration table;

   protected JdbcStringBasedCacheStoreConfiguration(String key2StringMapper, TableManipulationConfiguration table, ConnectionFactoryConfiguration connectionFactory,
         long lockAcquistionTimeout, int lockConcurrencyLevel, boolean purgeOnStartup, boolean purgeSynchronously, int purgerThreads, boolean fetchPersistentState,
         boolean ignoreModifications, TypedProperties properties, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(connectionFactory, lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications,
            properties, async, singletonStore);
      this.key2StringMapper = key2StringMapper;
      this.table = table;
   }

   public String key2StringMapper() {
      return key2StringMapper;
   }

   public TableManipulationConfiguration table() {
      return table;
   }

   @Override
   public JdbcStringBasedCacheStoreConfig adapt() {
      JdbcStringBasedCacheStoreConfig config = new JdbcStringBasedCacheStoreConfig();

      // StoreConfiguration
      LegacyConfigurationAdaptor.adapt(this, config);

      // ConnectionFactory
      ((LegacyConnectionFactoryAdaptor) connectionFactory()).adapt(config);

      // JdbcStringBasedCacheStoreConfiguration
      config.setKey2StringMapperClass(key2StringMapper);

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
      return "JdbcStringBasedCacheStoreConfiguration [key2StringMapper=" + key2StringMapper + ", table=" + table + ", connectionFactory()=" + connectionFactory()
            + ", lockAcquistionTimeout()=" + lockAcquistionTimeout() + ", lockConcurrencyLevel()=" + lockConcurrencyLevel() + ", async()=" + async() + ", singletonStore()="
            + singletonStore() + ", purgeOnStartup()=" + purgeOnStartup() + ", purgeSynchronously()=" + purgeSynchronously() + ", purgerThreads()=" + purgerThreads()
            + ", fetchPersistentState()=" + fetchPersistentState() + ", ignoreModifications()=" + ignoreModifications() + ", properties()=" + properties() + "]";
   }

}