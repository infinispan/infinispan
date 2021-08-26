package org.infinispan.persistence.sql.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.serializing.SerializedWith;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfiguration;
import org.infinispan.persistence.jdbc.common.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.sql.TableJdbcStore;

@BuiltBy(TableJdbcStoreConfigurationBuilder.class)
@ConfigurationFor(TableJdbcStore.class)
@SerializedWith(TableJdbcStoreConfigurationSerializer.class)
public class TableJdbcStoreConfiguration extends AbstractSchemaJdbcConfiguration {
   static final AttributeDefinition<String> TABLE_NAME = AttributeDefinition.builder(org.infinispan.persistence.jdbc.common.configuration.Attribute.TABLE_NAME, null, String.class).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TableJdbcStoreConfiguration.class, AbstractJdbcStoreConfiguration.attributeDefinitionSet(), TABLE_NAME);
   }

   private final Attribute<String> tableName;

   public TableJdbcStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async,
         ConnectionFactoryConfiguration connectionFactory, SchemaJdbcConfiguration schemaJdbcConfiguration) {
      super(attributes, async, connectionFactory, schemaJdbcConfiguration);
      tableName = attributes.attribute(TABLE_NAME);
   }

   public String tableName() {
      return tableName.get();
   }

   @Override
   public String toString() {
      return "TableJdbcStoreConfiguration [attributes=" + attributes +
            ", connectionFactory=" + connectionFactory() + ", async=" + async() + "]";
   }
}
