package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.persistence.jdbc.configuration.Element.CONNECTION_POOL;
import static org.infinispan.persistence.jdbc.configuration.Element.DATA_SOURCE;
import static org.infinispan.persistence.jdbc.configuration.Element.SIMPLE_CONNECTION;
import static org.infinispan.persistence.jdbc.configuration.Element.STRING_KEYED_TABLE;
import static org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration.KEY2STRING_MAPPER;
import static org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration.PROPERTIES;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.parsing.XmlConfigHelper;
import org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.persistence.keymappers.Key2StringMapper;

/**
 *
 * JdbcStringBasedStoreConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class JdbcStringBasedStoreConfigurationBuilder extends AbstractJdbcStoreConfigurationBuilder<JdbcStringBasedStoreConfiguration, JdbcStringBasedStoreConfigurationBuilder> implements ConfigurationBuilderInfo {
   private StringTableManipulationConfigurationBuilder table;

   public JdbcStringBasedStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, JdbcStringBasedStoreConfiguration.attributeDefinitionSet());
      table = new StringTableManipulationConfigurationBuilder(this);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ConfigurationBuilderInfo getBuilderInfo(String name, String qualifier) {
      if (name.equals(CONNECTION_POOL.getLocalName())) {
         return connectionPool();
      }
      if (name.equals(DATA_SOURCE.getLocalName())) {
         return dataSource();
      }
      if (name.equals(SIMPLE_CONNECTION.getLocalName())) {
         return simpleConnection();
      }
      if (name.equals(STRING_KEYED_TABLE.getLocalName())) {
         return table;
      }
      return null;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return JdbcStringBasedStoreConfiguration.ELEMENT_DEFINITION;
   }

   @Override
   public Collection<ConfigurationBuilderInfo> getChildrenInfo() {
      return Collections.singletonList(table);
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
