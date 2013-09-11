package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;

import java.util.Properties;

public abstract class AbstractJdbcStoreConfiguration extends AbstractStoreConfiguration {

   private final ConnectionFactoryConfiguration connectionFactory;
   private final boolean manageConnectionFactory;


   protected AbstractJdbcStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications,
                                            AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore,
                                            boolean preload, boolean shared, Properties properties,
                                            ConnectionFactoryConfiguration connectionFactory, boolean manageConnectionFactory) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
      this.connectionFactory = connectionFactory;
      this.manageConnectionFactory = manageConnectionFactory;
   }

   public ConnectionFactoryConfiguration connectionFactory() {
      return connectionFactory;
   }

   public boolean manageConnectionFactory() {
      return manageConnectionFactory;
   }
}
