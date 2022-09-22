package org.infinispan.persistence.sql.configuration;

import static org.infinispan.persistence.sql.configuration.SchemaJdbcConfiguration.EMBEDDED_KEY;
import static org.infinispan.persistence.sql.configuration.SchemaJdbcConfiguration.KEY_MESSAGE_NAME;
import static org.infinispan.persistence.sql.configuration.SchemaJdbcConfiguration.MESSAGE_NAME;
import static org.infinispan.persistence.sql.configuration.SchemaJdbcConfiguration.PACKAGE;

import java.util.Objects;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.configuration.AbstractJdbcStoreConfigurationChildBuilder;

public class SchemaJdbcConfigurationBuilder<S extends AbstractJdbcStoreConfigurationBuilder<?, S>> extends AbstractJdbcStoreConfigurationChildBuilder<S> implements Builder<SchemaJdbcConfiguration> {

   private final AttributeSet attributes;

   protected SchemaJdbcConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder<?, S> builder) {
      super(builder);
      this.attributes = SchemaJdbcConfiguration.attributeDefinitionSet();
   }

   /**
    * The protobuf message name to use to marshall the cache entry to the database. If the value is a single column,
    * this is not required.
    *
    * @param messageName the protobuf message name for the value
    * @return this
    */
   public SchemaJdbcConfigurationBuilder<S> messageName(String messageName) {
      attributes.attribute(MESSAGE_NAME).set(messageName);
      return this;
   }

   /**
    * The protobuf message name to use to marshall the cache key to the database. If the key is a single column, this is
    * not required.
    *
    * @param keyMessageName the protobuf message name for the key
    * @return this
    */
   public SchemaJdbcConfigurationBuilder<S> keyMessageName(String keyMessageName) {
      attributes.attribute(KEY_MESSAGE_NAME).set(keyMessageName);
      return this;
   }

   /**
    * Sets the package name to be used when determining which schemas are used with {@link #messageName(String)} and
    * {@link #keyMessageName(String)}.
    *
    * @param packageName the package for the message or key message
    * @return this
    */
   public SchemaJdbcConfigurationBuilder<S> packageName(String packageName) {
      attributes.attribute(PACKAGE).set(Objects.requireNonNull(packageName));
      return this;
   }

   /**
    * Whether the key column(s) should be also written into the value object. When this is enabled, {@link
    * #messageName(String) <b>must</b> also be configured.
    *
    * @param embeddedKey whether the key is embedded in the value
    * @return this
    */
   public SchemaJdbcConfigurationBuilder<S> embeddedKey(boolean embeddedKey) {
      attributes.attribute(EMBEDDED_KEY).set(embeddedKey);
      return this;
   }

   @Override
   public void validate() {
      Boolean embedKey = attributes.attribute(EMBEDDED_KEY).get();
      if (embedKey != null && embedKey) {
         if (attributes.attribute(MESSAGE_NAME).get() == null) {
            throw org.infinispan.persistence.jdbc.common.logging.Log.CONFIG.messageNameRequiredIfEmbeddedKey();
         }
      }
   }

   @Override
   public SchemaJdbcConfiguration create() {
      return new SchemaJdbcConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(SchemaJdbcConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
