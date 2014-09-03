package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.persistence.jdbc.DatabaseType;

import java.util.Properties;

public abstract class AbstractJdbcStoreConfiguration extends AbstractStoreConfiguration {

   private final ConnectionFactoryConfiguration connectionFactory;
   private final boolean manageConnectionFactory;
   private final DatabaseType databaseType;

   protected AbstractJdbcStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications,
                                            AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore,
                                            boolean preload, boolean shared, Properties properties,
                                            ConnectionFactoryConfiguration connectionFactory, boolean manageConnectionFactory, DatabaseType databaseType) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
      this.connectionFactory = connectionFactory;
      this.manageConnectionFactory = manageConnectionFactory;
      this.databaseType = databaseType;
   }

   public ConnectionFactoryConfiguration connectionFactory() {
      return connectionFactory;
   }

   public boolean manageConnectionFactory() {
      return manageConnectionFactory;
   }

   public DatabaseType dialect() {
      return databaseType;
   }

   @Override
   public String toString() {
      return "AbstractJdbcStoreConfiguration{" +
            ", connectionFactory=" + connectionFactory +
            ", managedConnectionFactory=" + manageConnectionFactory +
            ", dialect=" + databaseType +
            "}";
   }

}
