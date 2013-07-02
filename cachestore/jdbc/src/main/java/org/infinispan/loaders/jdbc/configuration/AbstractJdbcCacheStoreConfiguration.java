package org.infinispan.loaders.jdbc.configuration;

import org.infinispan.configuration.cache.AbstractLockSupportStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.commons.util.TypedProperties;

public abstract class AbstractJdbcCacheStoreConfiguration extends AbstractLockSupportStoreConfiguration {

   private final ConnectionFactoryConfiguration connectionFactory;

   AbstractJdbcCacheStoreConfiguration(ConnectionFactoryConfiguration connectionFactory, long lockAcquistionTimeout,
         int lockConcurrencyLevel, boolean purgeOnStartup, boolean purgeSynchronously, int purgerThreads,
         boolean fetchPersistentState, boolean ignoreModifications, TypedProperties properties,
         AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup, purgeSynchronously, purgerThreads,
            fetchPersistentState, ignoreModifications, properties, async, singletonStore);
      this.connectionFactory = connectionFactory;
   }

   public ConnectionFactoryConfiguration connectionFactory() {
      return connectionFactory;
   }

}
