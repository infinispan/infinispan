package org.infinispan.persistence.jdbc.configuration;

import java.util.Map;
import java.util.Properties;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.XmlConfigHelper;
import org.infinispan.commons.util.TypedProperties;
import static org.infinispan.persistence.jdbc.configuration.JdbcBinaryStoreConfiguration.*;

public class JdbcBinaryStoreConfigurationBuilder extends
                                                      AbstractJdbcStoreConfigurationBuilder<JdbcBinaryStoreConfiguration, JdbcBinaryStoreConfigurationBuilder> {
   protected final BinaryTableManipulationConfigurationBuilder table;

   public JdbcBinaryStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, JdbcBinaryStoreConfiguration.attributeDefinitionSet());
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
      Map<Object, Object> unrecognized = XmlConfigHelper.setAttributes(attributes, props, false, false);
      unrecognized = XmlConfigHelper.setAttributes(table.attributes(), unrecognized, false, false);
      XmlConfigHelper.showUnrecognizedAttributes(unrecognized);
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(props));
      return this;
   }

   public JdbcBinaryStoreConfigurationBuilder lockAcquisitionTimeout(long lockAcquisitionTimeout) {
      attributes.attribute(LOCK_ACQUISITION_TIMEOUT).set(lockAcquisitionTimeout);
      return self();
   }


   public JdbcBinaryStoreConfigurationBuilder concurrencyLevel(int concurrencyLevel) {
      attributes.attribute(CONCURRENCY_LEVEL).set(concurrencyLevel);
      return self();
   }

   @Override
   public JdbcBinaryStoreConfiguration create() {
      return new JdbcBinaryStoreConfiguration(attributes.protect(), async.create(), singletonStore.create(), connectionFactory != null ? connectionFactory.create() : null, table.create());
   }

   @Override
   public JdbcBinaryStoreConfigurationBuilder read(JdbcBinaryStoreConfiguration template) {
      super.read(template);
      table.read(template.table());
      return this;
   }

   @Override
   public String toString() {
      return "JdbcBinaryStoreConfigurationBuilder [table=" + table + ", connectionFactory=" + connectionFactory + ", attributes=" + attributes + ", async=" + async
            + ", singletonStore=" + singletonStore + "]";
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
