package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.persistence.jdbc.Dialect;
import org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStore;

import java.util.Properties;

@BuiltBy(JdbcStringBasedStoreConfigurationBuilder.class)
@ConfigurationFor(JdbcStringBasedStore.class)
public class JdbcStringBasedStoreConfiguration extends AbstractJdbcStoreConfiguration {

   private final String key2StringMapper;

   private final TableManipulationConfiguration table;

   public JdbcStringBasedStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications,
                                            AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore,
                                            boolean preload, boolean shared, Properties properties,
                                            ConnectionFactoryConfiguration connectionFactory, boolean manageConnectionFactory,
                                            String key2StringMapper, TableManipulationConfiguration table, Dialect dialect) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties, connectionFactory, manageConnectionFactory, dialect);
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
   public String toString() {
      return "JdbcStringBasedStoreConfiguration{" +
            "key2StringMapper='" + key2StringMapper + '\'' +
            ", table=" + table +
            ", " + super.toString() +
            '}';
   }
}