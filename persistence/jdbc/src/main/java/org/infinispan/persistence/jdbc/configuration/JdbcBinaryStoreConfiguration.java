package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.persistence.jdbc.binary.JdbcBinaryStore;

import java.util.Properties;

@BuiltBy(JdbcBinaryStoreConfigurationBuilder.class)
@ConfigurationFor(JdbcBinaryStore.class)
public class JdbcBinaryStoreConfiguration extends AbstractJdbcStoreConfiguration {

   private final TableManipulationConfiguration table;

   private int concurrencyLevel;

   private long lockAcquisitionTimeout;

   public JdbcBinaryStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore, boolean preload, boolean shared, Properties properties, ConnectionFactoryConfiguration connectionFactory, boolean manageConnectionFactory, TableManipulationConfiguration table, int concurrencyLevel, long lockAcquisitionTimeout) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties, connectionFactory, manageConnectionFactory);
      this.table = table;
      this.concurrencyLevel = concurrencyLevel;
      this.lockAcquisitionTimeout = lockAcquisitionTimeout;
   }

   public TableManipulationConfiguration table() {
      return table;
   }

   public int lockConcurrencyLevel() {
      return concurrencyLevel;
   }

   public long lockAcquisitionTimeout() {
      return lockAcquisitionTimeout;
   }


   @Override
   public String toString() {
      return "JdbcBinaryStoreConfiguration{" +
            "table=" + table +
            ", concurrencyLevel=" + concurrencyLevel +
            ", lockAcquistionTimeout=" + lockAcquisitionTimeout +
             ", " + super.toString() +
            '}';
   }
}
