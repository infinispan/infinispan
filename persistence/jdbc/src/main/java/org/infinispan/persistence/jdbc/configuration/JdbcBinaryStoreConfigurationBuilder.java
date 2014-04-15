package org.infinispan.persistence.jdbc.configuration;

import java.util.Map;
import java.util.Properties;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.XmlConfigHelper;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.persistence.jdbc.Dialect;

public class JdbcBinaryStoreConfigurationBuilder extends
                                                      AbstractJdbcStoreConfigurationBuilder<JdbcBinaryStoreConfiguration, JdbcBinaryStoreConfigurationBuilder> {
   public static final int DEFAULT_CONCURRENCY_LEVEL = 2048;
   public static final int DEFAULT_LOCK_ACQUISITION_TIMEOUT = 60000;

   protected final BinaryTableManipulationConfigurationBuilder table;

   private int concurrencyLevel = DEFAULT_CONCURRENCY_LEVEL;

   private long lockAcquisitionTimeout = DEFAULT_LOCK_ACQUISITION_TIMEOUT;

   public JdbcBinaryStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
      this.table = new BinaryTableManipulationConfigurationBuilder(this);
   }

   @Override
   public JdbcBinaryStoreConfigurationBuilder self() {
      return this;
   }

   /**
    * Allows configuration of table-specific parameters such as column names and types
    */
   public BinaryTableManipulationConfigurationBuilder table() {
      return table;
   }

   @Override
   public JdbcBinaryStoreConfigurationBuilder withProperties(Properties props) {
      Map<Object, Object> unrecognized = XmlConfigHelper.setValues(this, props, false, false);
      unrecognized = XmlConfigHelper.setValues(table, unrecognized, false, false);
      XmlConfigHelper.showUnrecognizedAttributes(unrecognized);
      this.properties = props;
      return this;
   }

   public JdbcBinaryStoreConfigurationBuilder lockAcquisitionTimeout(long lockAcquisitionTimeout) {
      this.lockAcquisitionTimeout = lockAcquisitionTimeout;
      return self();
   }


   public JdbcBinaryStoreConfigurationBuilder concurrencyLevel(int concurrencyLevel) {
      this.concurrencyLevel = concurrencyLevel;
      return self();
   }

   @Override
   public JdbcBinaryStoreConfiguration create() {
      ConnectionFactoryConfiguration cf = connectionFactory != null ? connectionFactory.create() : null;
      return new JdbcBinaryStoreConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications, async.create(),
                                              singletonStore.create(), preload, shared, TypedProperties.toTypedProperties(properties), cf,
                                              manageConnectionFactory, table.create(), concurrencyLevel, lockAcquisitionTimeout, dialect);
   }

   @Override
   public JdbcBinaryStoreConfigurationBuilder read(JdbcBinaryStoreConfiguration template) {
      super.read(template);

      this.table.read(template.table());
      this.lockAcquisitionTimeout = template.lockAcquisitionTimeout();
      this.concurrencyLevel = template.lockConcurrencyLevel();

      return this;
   }

   public class BinaryTableManipulationConfigurationBuilder extends
         TableManipulationConfigurationBuilder<JdbcBinaryStoreConfigurationBuilder, BinaryTableManipulationConfigurationBuilder> {

      BinaryTableManipulationConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder<?, JdbcBinaryStoreConfigurationBuilder> builder) {
         super(builder);
      }

      @Override
      public PooledConnectionFactoryConfigurationBuilder<JdbcBinaryStoreConfigurationBuilder> connectionPool() {
         return JdbcBinaryStoreConfigurationBuilder.this.connectionPool();
      }

      @Override
      public ManagedConnectionFactoryConfigurationBuilder<JdbcBinaryStoreConfigurationBuilder> dataSource() {
         return JdbcBinaryStoreConfigurationBuilder.this.dataSource();
      }

      @Override
      public BinaryTableManipulationConfigurationBuilder self() {
         return this;
      }
   }
}
