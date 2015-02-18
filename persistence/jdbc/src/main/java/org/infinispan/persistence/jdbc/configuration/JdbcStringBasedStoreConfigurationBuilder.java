package org.infinispan.persistence.jdbc.configuration;

import java.util.Map;
import java.util.Properties;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.XmlConfigHelper;
import org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.persistence.keymappers.Key2StringMapper;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;

import static org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration.*;

/**
 *
 * JdbcStringBasedStoreConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class JdbcStringBasedStoreConfigurationBuilder extends AbstractJdbcStoreConfigurationBuilder<JdbcStringBasedStoreConfiguration, JdbcStringBasedStoreConfigurationBuilder> {
   private StringTableManipulationConfigurationBuilder table;

   public JdbcStringBasedStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, JdbcStringBasedStoreConfiguration.attributeDefinitionSet());
      table = new StringTableManipulationConfigurationBuilder(this);
   }

   @Override
   public JdbcStringBasedStoreConfigurationBuilder self() {
      return this;
   }

   /**
    * The class name of a {@link Key2StringMapper} to use for mapping keys to strings suitable for
    * storage in a database table. Defaults to {@link DefaultTwoWayKey2StringMapper}
    */
   public JdbcStringBasedStoreConfigurationBuilder key2StringMapper(String key2StringMapper) {
      attributes.attribute(KEY2STRING_MAPPER).set(key2StringMapper);
      return this;
   }

   /**
    * The class of a {@link Key2StringMapper} to use for mapping keys to strings suitable for
    * storage in a database table. Defaults to {@link DefaultTwoWayKey2StringMapper}
    */
   public JdbcStringBasedStoreConfigurationBuilder key2StringMapper(Class<? extends Key2StringMapper> klass) {
      key2StringMapper(klass.getName());
      return this;
   }

   /**
    * Allows configuration of table-specific parameters such as column names and types
    */
   public StringTableManipulationConfigurationBuilder table() {
      return table;
   }

   @Override
   public JdbcStringBasedStoreConfigurationBuilder withProperties(Properties props) {
      Map<Object, Object> unrecognized = XmlConfigHelper.setAttributes(attributes, props, false, false);
      unrecognized = XmlConfigHelper.setAttributes(table.attributes(), unrecognized, false, false);
      XmlConfigHelper.showUnrecognizedAttributes(unrecognized);
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(props));
      return this;
   }

   @Override
   public JdbcStringBasedStoreConfiguration create() {
      return new JdbcStringBasedStoreConfiguration(attributes.protect(), async.create(), singletonStore.create(), connectionFactory != null ? connectionFactory.create() : null,
            table.create());
   }

   @Override
   public Builder<?> read(JdbcStringBasedStoreConfiguration template) {
      super.read(template);
      this.table.read(template.table());
      return this;
   }

   public class StringTableManipulationConfigurationBuilder extends
         TableManipulationConfigurationBuilder<JdbcStringBasedStoreConfigurationBuilder, StringTableManipulationConfigurationBuilder> {

      StringTableManipulationConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder<?, JdbcStringBasedStoreConfigurationBuilder> builder) {
         super(builder);
      }

      @Override
      public StringTableManipulationConfigurationBuilder self() {
         return this;
      }

      @Override
      public PooledConnectionFactoryConfigurationBuilder<JdbcStringBasedStoreConfigurationBuilder> connectionPool() {
         return JdbcStringBasedStoreConfigurationBuilder.this.connectionPool();
      }

      @Override
      public ManagedConnectionFactoryConfigurationBuilder<JdbcStringBasedStoreConfigurationBuilder> dataSource() {
         return JdbcStringBasedStoreConfigurationBuilder.this.dataSource();
      }
   }

   @Override
   public String toString() {
      return "JdbcStringBasedStoreConfigurationBuilder [table=" + table + ", connectionFactory=" + connectionFactory + ", attributes=" + attributes + ", async=" + async
            + ", singletonStore=" + singletonStore + "]";
   }

}
