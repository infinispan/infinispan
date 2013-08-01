package org.infinispan.loaders.jdbc.configuration;

import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.commons.util.TypedProperties;

public class JdbcBinaryCacheStoreConfigurationBuilder extends
      AbstractJdbcCacheStoreConfigurationBuilder<JdbcBinaryCacheStoreConfiguration, JdbcBinaryCacheStoreConfigurationBuilder> {
   protected final BinaryTableManipulationConfigurationBuilder table;

   public JdbcBinaryCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
      this.table = new BinaryTableManipulationConfigurationBuilder(this);
   }

   @Override
   public JdbcBinaryCacheStoreConfigurationBuilder self() {
      return this;
   }

   /**
    * Allows configuration of table-specific parameters such as column names and types
    */
   public BinaryTableManipulationConfigurationBuilder table() {
      return table;
   }

   @Override
   public void validate() {
      super.validate();
   }

   @Override
   public JdbcBinaryCacheStoreConfiguration create() {
      return new JdbcBinaryCacheStoreConfiguration(table.create(), connectionFactory != null ? connectionFactory.create() : null, manageConnectionFactory, lockAcquistionTimeout, lockConcurrencyLevel, purgeOnStartup, purgeSynchronously,
            purgerThreads, fetchPersistentState, ignoreModifications, TypedProperties.toTypedProperties(properties), async.create(), singletonStore.create());
   }

   @Override
   public JdbcBinaryCacheStoreConfigurationBuilder read(JdbcBinaryCacheStoreConfiguration template) {
      super.readInternal(template);
      this.table.read(template.table());

      return this;
   }

   public class BinaryTableManipulationConfigurationBuilder extends
         TableManipulationConfigurationBuilder<JdbcBinaryCacheStoreConfigurationBuilder, BinaryTableManipulationConfigurationBuilder> {

      BinaryTableManipulationConfigurationBuilder(AbstractJdbcCacheStoreConfigurationBuilder<?, JdbcBinaryCacheStoreConfigurationBuilder> builder) {
         super(builder);
      }

      @Override
      public PooledConnectionFactoryConfigurationBuilder<JdbcBinaryCacheStoreConfigurationBuilder> connectionPool() {
         return JdbcBinaryCacheStoreConfigurationBuilder.this.connectionPool();
      }

      @Override
      public ManagedConnectionFactoryConfigurationBuilder<JdbcBinaryCacheStoreConfigurationBuilder> dataSource() {
         return JdbcBinaryCacheStoreConfigurationBuilder.this.dataSource();
      }

      @Override
      public BinaryTableManipulationConfigurationBuilder self() {
         return this;
      }
   }
}
