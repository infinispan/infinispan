package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.persistence.jdbc.DatabaseType;
import org.infinispan.persistence.jdbc.mixed.JdbcMixedStore;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;

import java.util.Properties;

/**
 *
 * JdbcMixedStoreConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@BuiltBy(JdbcMixedStoreConfigurationBuilder.class)
@ConfigurationFor(JdbcMixedStore.class)
public class JdbcMixedStoreConfiguration extends AbstractJdbcStoreConfiguration {

   private final int batchSize;
   private final int fetchSize;
   private final DatabaseType databaseType;
   private final TableManipulationConfiguration binaryTable;
   private final TableManipulationConfiguration stringTable;
   private final String key2StringMapper;
   private final int lockConcurrencyLevel;
   private final long lockAcquisitionTimeout;




   public JdbcMixedStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications,
                                      AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore,
                                      boolean preload, boolean shared, Properties properties,
                                      ConnectionFactoryConfiguration connectionFactory, boolean manageConnectionFactory,
                                      int batchSize, int fetchSize, DatabaseType databaseType,
                                      TableManipulationConfiguration binaryTable,
                                      TableManipulationConfiguration stringTable, String key2StringMapper, int lockConcurrencyLevel, long lockAcquisitionTimeout) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties, connectionFactory, manageConnectionFactory);
      this.batchSize = batchSize;
      this.fetchSize = fetchSize;
      this.databaseType = databaseType;
      this.binaryTable = binaryTable;
      this.stringTable = stringTable;
      this.key2StringMapper = key2StringMapper;
      this.lockConcurrencyLevel = lockConcurrencyLevel;
      this.lockAcquisitionTimeout = lockAcquisitionTimeout;
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

   public int lockConcurrencyLevel() {
      return lockConcurrencyLevel;
   }

   public long lockAcquisitionTimeout() {
      return lockAcquisitionTimeout;
   }

   @Override
   public String toString() {
      return "JdbcMixedStoreConfiguration{" +
            "batchSize=" + batchSize +
            ", fetchSize=" + fetchSize +
            ", databaseType=" + databaseType +
            ", binaryTable=" + binaryTable +
            ", stringTable=" + stringTable +
            ", key2StringMapper='" + key2StringMapper + '\'' + super.toString() +
            '}';
   }
}