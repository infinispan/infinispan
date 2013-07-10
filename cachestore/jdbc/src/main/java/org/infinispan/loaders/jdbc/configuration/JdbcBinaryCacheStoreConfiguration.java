package org.infinispan.loaders.jdbc.configuration;

import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStore;

@BuiltBy(JdbcBinaryCacheStoreConfigurationBuilder.class)
@ConfigurationFor(JdbcBinaryCacheStore.class)
public class JdbcBinaryCacheStoreConfiguration extends AbstractJdbcCacheStoreConfiguration {

   private final TableManipulationConfiguration table;

   JdbcBinaryCacheStoreConfiguration(TableManipulationConfiguration table, ConnectionFactoryConfiguration connectionFactory, boolean manageConnectionFactory,
         long lockAcquistionTimeout, int lockConcurrencyLevel, boolean purgeOnStartup, boolean purgeSynchronously, int purgerThreads, boolean fetchPersistentState,
         boolean ignoreModifications, TypedProperties properties, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(connectionFactory, manageConnectionFactory, lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup, purgeSynchronously, purgerThreads, fetchPersistentState, ignoreModifications,
            properties, async, singletonStore);
      this.table = table;
   }

   public TableManipulationConfiguration table() {
      return table;
   }

   @Override
   public String toString() {
      return "JdbcBinaryCacheStoreConfiguration [table=" + table + ", connectionFactory()=" + connectionFactory() + ", manageConnectionFactory()=" + manageConnectionFactory()
            + ", lockAcquistionTimeout()=" + lockAcquistionTimeout() + ", lockConcurrencyLevel()=" + lockConcurrencyLevel() + ", async()=" + async() + ", singletonStore()="
            + singletonStore() + ", purgeOnStartup()=" + purgeOnStartup() + ", purgeSynchronously()=" + purgeSynchronously() + ", purgerThreads()=" + purgerThreads()
            + ", fetchPersistentState()=" + fetchPersistentState() + ", ignoreModifications()=" + ignoreModifications() + ", properties()=" + properties() + "]";
   }
}
