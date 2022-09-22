package org.infinispan.persistence.sql.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.logging.Log;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfigurationBuilder;

public abstract class AbstractSchemaJdbcConfigurationBuilder<T extends AbstractSchemaJdbcConfiguration,
      S extends AbstractSchemaJdbcConfigurationBuilder<T, S>> extends AbstractJdbcStoreConfigurationBuilder<T, S> {
   protected final SchemaJdbcConfigurationBuilder<S> schemaBuilder = new SchemaJdbcConfigurationBuilder<>(this);

   public AbstractSchemaJdbcConfigurationBuilder(PersistenceConfigurationBuilder builder, AttributeSet attributes) {
      super(builder, attributes);
   }

   /**
    * Retrieves the schema configuration builder
    *
    * @return builder to configure the schema
    * @deprecated use {@link #schema()} instead
    */
   @Deprecated
   public SchemaJdbcConfigurationBuilder<S> schemaJdbcConfigurationBuilder() {
      return schemaBuilder;
   }

   public SchemaJdbcConfigurationBuilder<S> schema() {
      return schemaBuilder;
   }

   @Override
   public void validate() {
      super.validate();
      schemaBuilder.validate();

      Attribute<Boolean> segmentedAttr = attributes.attribute(AbstractStoreConfiguration.SEGMENTED);
      if (!segmentedAttr.isModified()) {
         Log.CONFIG.debugf("%s is defaulting to not being segmented", getClass().getSimpleName());
         segmentedAttr.set(Boolean.FALSE);
      } else if (segmentedAttr.get()) {
         throw org.infinispan.util.logging.Log.CONFIG.storeDoesNotSupportBeingSegmented(getClass().getSimpleName());
      }
   }

   @Override
   public Builder<?> read(T template) {
      super.read(template);
      schemaBuilder.read(template.schema());
      return this;
   }
}
