package org.infinispan.persistence.sql.configuration;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfiguration;
import org.infinispan.persistence.jdbc.common.configuration.ConnectionFactoryConfiguration;

public class AbstractSchemaJdbcConfiguration extends AbstractJdbcStoreConfiguration {
   static final String NAMESPACE = Parser.NAMESPACE + "store:sql:";

   private final SchemaJdbcConfiguration schemaJdbcConfiguration;

   protected AbstractSchemaJdbcConfiguration(AttributeSet attributes, AsyncStoreConfiguration async,
         ConnectionFactoryConfiguration connectionFactory, SchemaJdbcConfiguration schemaJdbcConfiguration) {
      super(attributes, async, connectionFactory);

      this.schemaJdbcConfiguration = schemaJdbcConfiguration;
   }

   public SchemaJdbcConfiguration schema() {
      return schemaJdbcConfiguration;
   }

   /**
    * @deprecated use {@link #schema()} instead.
    */
   @Deprecated
   public SchemaJdbcConfiguration getSchemaJdbcConfiguration() {
      return schemaJdbcConfiguration;
   }
}
