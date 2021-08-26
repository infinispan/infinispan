package org.infinispan.persistence.sql.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.serializing.SerializedWith;
import org.infinispan.persistence.jdbc.common.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.sql.QueriesJdbcStore;

@BuiltBy(QueriesJdbcStoreConfigurationBuilder.class)
@ConfigurationFor(QueriesJdbcStore.class)
@SerializedWith(QueriesJdbcStoreConfigurationSerializer.class)
public class QueriesJdbcStoreConfiguration extends AbstractSchemaJdbcConfiguration {
   static final AttributeDefinition<String> KEY_COLUMNS = AttributeDefinition.builder(org.infinispan.persistence.jdbc.common.configuration.Attribute.KEY_COLUMNS, null, String.class).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(QueriesJdbcStoreConfiguration.class, AbstractSchemaJdbcConfiguration.attributeDefinitionSet(), KEY_COLUMNS);
   }

   private final QueriesJdbcConfiguration queriesJdbcConfiguration;

   private final Attribute<String> keyColumns;

   public QueriesJdbcStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async,
         ConnectionFactoryConfiguration connectionFactory, SchemaJdbcConfiguration schemaJdbcConfiguration,
         QueriesJdbcConfiguration queriesJdbcConfiguration) {
      super(attributes, async, connectionFactory, schemaJdbcConfiguration);
      this.queriesJdbcConfiguration = queriesJdbcConfiguration;
      keyColumns = attributes.attribute(KEY_COLUMNS);
   }

   public String keyColumns() {
      return keyColumns.get();
   }

   public QueriesJdbcConfiguration getQueriesJdbcConfiguration() {
      return queriesJdbcConfiguration;
   }

   @Override
   public String toString() {
      return "QueriesJdbcStoreConfiguration [attributes=" + attributes +
            ", connectionFactory=" + connectionFactory() + ", async=" + async() + "]";
   }
}
