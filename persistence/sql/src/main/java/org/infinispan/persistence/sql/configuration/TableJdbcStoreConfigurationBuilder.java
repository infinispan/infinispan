package org.infinispan.persistence.sql.configuration;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * TableJdbcStoreConfigurationBuilder.
 *
 * @author William Burns
 * @since 13.0
 */
public class TableJdbcStoreConfigurationBuilder extends AbstractSchemaJdbcConfigurationBuilder<TableJdbcStoreConfiguration, TableJdbcStoreConfigurationBuilder> {

   public TableJdbcStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, TableJdbcStoreConfiguration.attributeDefinitionSet());
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public TableJdbcStoreConfigurationBuilder self() {
      return this;
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      super.validate(globalConfig);
      if (attributes.attribute(TableJdbcStoreConfiguration.TABLE_NAME).get() == null) {
         throw org.infinispan.persistence.jdbc.common.logging.Log.CONFIG.tableNameMissing();
      }
   }

   /**
    * Configures the table name to use for this store
    *
    * @param tableName table to use
    * @return this
    */
   public TableJdbcStoreConfigurationBuilder tableName(String tableName) {
      attributes.attribute(TableJdbcStoreConfiguration.TABLE_NAME).set(tableName);
      return this;
   }

   @Override
   public TableJdbcStoreConfiguration create() {
      return new TableJdbcStoreConfiguration(attributes.protect(), async.create(),
            connectionFactory != null ? connectionFactory.create() : null,
            schemaBuilder.create());
   }

   @Override
   public String toString() {
      return "TableJdbcStoreConfigurationBuilder [connectionFactory=" + connectionFactory +
             ", attributes=" + attributes + ", async=" + async + "]";
   }

}
