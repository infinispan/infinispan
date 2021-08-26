package org.infinispan.persistence.sql.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

/**
 * QueriesJdbcStoreConfigurationBuilder.
 *
 * @author William Burns
 * @since 13.0
 */
public class QueriesJdbcStoreConfigurationBuilder extends AbstractSchemaJdbcConfigurationBuilder<QueriesJdbcStoreConfiguration, QueriesJdbcStoreConfigurationBuilder> {

   private final QueriesJdbcConfigurationBuilder queriesJdbcConfigurationBuilder = new QueriesJdbcConfigurationBuilder();

   public QueriesJdbcStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, QueriesJdbcStoreConfiguration.attributeDefinitionSet());
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public QueriesJdbcStoreConfigurationBuilder self() {
      return this;
   }

   @Override
   public void validate() {
      super.validate();
      queriesJdbcConfigurationBuilder.validate(attributes.attribute(AbstractStoreConfiguration.READ_ONLY).get());

      Attribute<String> keyAttr = attributes.attribute(QueriesJdbcStoreConfiguration.KEY_COLUMNS);
      if (!keyAttr.isModified() || keyAttr.isNull() || keyAttr.get().isEmpty()) {
         throw org.infinispan.persistence.jdbc.common.logging.Log.CONFIG.keyColumnsRequired();
      }
   }

   public QueriesJdbcConfigurationBuilder queriesJdbcConfigurationBuilder() {
      return queriesJdbcConfigurationBuilder;
   }

   public QueriesJdbcStoreConfigurationBuilder keyColumns(String keyColumns) {
      attributes.attribute(QueriesJdbcStoreConfiguration.KEY_COLUMNS).set(keyColumns);
      return this;
   }

   @Override
   public QueriesJdbcStoreConfiguration create() {
      return new QueriesJdbcStoreConfiguration(attributes.protect(), async.create(),
            connectionFactory != null ? connectionFactory.create() : null,
            schemaJdbcConfigurationBuilder.create(), queriesJdbcConfigurationBuilder.create());
   }

   @Override
   public Builder<?> read(QueriesJdbcStoreConfiguration template) {
      super.read(template);
      queriesJdbcConfigurationBuilder.read(template.getQueriesJdbcConfiguration());
      return this;
   }

   @Override
   public String toString() {
      return "QueriesJdbcStoreConfigurationBuilder [connectionFactory=" + connectionFactory +
            ", attributes=" + attributes + ", async=" + async + "]";
   }

}
