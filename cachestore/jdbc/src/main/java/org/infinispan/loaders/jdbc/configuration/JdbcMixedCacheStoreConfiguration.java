package org.infinispan.loaders.jdbc.configuration;

import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.loaders.jdbc.DatabaseType;
import org.infinispan.loaders.jdbc.mixed.JdbcMixedCacheStore;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.util.TypedProperties;

/**
 *
 * JdbcMixedCacheStoreConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@BuiltBy(JdbcMixedCacheStoreConfigurationBuilder.class)
@ConfigurationFor(JdbcMixedCacheStore.class)
public class JdbcMixedCacheStoreConfiguration extends AbstractJdbcCacheStoreConfiguration {

   private final int batchSize;
   private final int fetchSize;
   private final DatabaseType databaseType;
   private final TableManipulationConfiguration binaryTable;
   private final TableManipulationConfiguration stringTable;
   private final String key2StringMapper;

   protected JdbcMixedCacheStoreConfiguration(int batchSize, int fetchSize, DatabaseType databaseType, String key2StringMapper, TableManipulationConfiguration binaryTable, TableManipulationConfiguration stringTable,
         ConnectionFactoryConfiguration connectionFactory, boolean manageConnectionFactory, long lockAcquistionTimeout, int lockConcurrencyLevel, boolean purgeOnStartup, boolean purgeSynchronously,
         int purgerThreads, boolean fetchPersistentState, boolean ignoreModifications, TypedProperties properties, AsyncStoreConfiguration async,
         SingletonStoreConfiguration singletonStore) {
      super(connectionFactory, manageConnectionFactory, lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications,
            properties, async, singletonStore);
      this.databaseType = databaseType;
      this.batchSize = batchSize;
      this.fetchSize = fetchSize;
      this.key2StringMapper = key2StringMapper;
      this.binaryTable = binaryTable;
      this.stringTable = stringTable;
   }

   public String key2StringMapper() {
      return key2StringMapper;
   }

   public TableManipulationConfiguration binaryTable() {
      return binaryTable;
   }

   public TableManipulationConfiguration stringTable() {
      return stringTable;
   }

   public int batchSize() {
      return batchSize;
   }

   public int fetchSize() {
      return fetchSize;
   }

   public DatabaseType databaseType() {
      return databaseType;
   }

   @Override
   public String toString() {
      return "JdbcMixedCacheStoreConfiguration [batchSize=" + batchSize + ", fetchSize=" + fetchSize + ", databaseType=" + databaseType + ", binaryTable=" + binaryTable
            + ", stringTable=" + stringTable + ", key2StringMapper=" + key2StringMapper + ", connectionFactory()=" + connectionFactory() + ", manageConnectionFactory()="
            + manageConnectionFactory() + ", lockAcquistionTimeout()=" + lockAcquistionTimeout() + ", lockConcurrencyLevel()=" + lockConcurrencyLevel() + ", async()=" + async()
            + ", singletonStore()=" + singletonStore() + ", purgeOnStartup()=" + purgeOnStartup() + ", purgeSynchronously()=" + purgeSynchronously() + ", purgerThreads()="
            + purgerThreads() + ", fetchPersistentState()=" + fetchPersistentState() + ", ignoreModifications()=" + ignoreModifications() + ", properties()=" + properties() + "]";
   }



}