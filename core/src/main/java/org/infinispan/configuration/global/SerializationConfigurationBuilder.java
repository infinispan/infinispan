package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.SerializationConfiguration.MARSHALLER;
import static org.infinispan.configuration.global.SerializationConfiguration.SCHEMA_COMPATIBILITY;
import static org.infinispan.configuration.global.SerializationConfiguration.SERIALIZATION_CONTEXT_INITIALIZERS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.config.Configuration;

/**
 * Configures serialization and marshalling settings.
 */
public class SerializationConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<SerializationConfiguration> {
   private final AttributeSet attributes;
   private final AllowListConfigurationBuilder allowListBuilder;

   SerializationConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      this.allowListBuilder = new AllowListConfigurationBuilder(globalConfig);
      this.attributes = SerializationConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Set the marshaller instance that will marshall and unmarshall cache entries.
    *
    * @param marshaller
    */
   public SerializationConfigurationBuilder marshaller(Marshaller marshaller) {
      attributes.attribute(MARSHALLER).set(marshaller);
      return this;
   }

   public Marshaller getMarshaller() {
      return attributes.attribute(MARSHALLER).get();
   }

   public SerializationConfigurationBuilder addContextInitializer(SerializationContextInitializer sci) {
      if (sci == null)
         throw new CacheConfigurationException("SerializationContextInitializer cannot be null");
      attributes.attribute(SERIALIZATION_CONTEXT_INITIALIZERS).computeIfAbsent(ArrayList::new).add(sci);
      return this;
   }

   public SerializationConfigurationBuilder addContextInitializers(SerializationContextInitializer... scis) {
      return addContextInitializers(Arrays.asList(scis));
   }

   public SerializationConfigurationBuilder addContextInitializers(List<SerializationContextInitializer> scis) {
      attributes.attribute(SERIALIZATION_CONTEXT_INITIALIZERS).computeIfAbsent(ArrayList::new).addAll(scis);
      return this;
   }

   public AllowListConfigurationBuilder allowList() {
      return allowListBuilder;
   }

   public SerializationConfigurationBuilder schemaCompatibilityValidation(Configuration.SchemaValidation schemaValidation) {
      attributes.attribute(SCHEMA_COMPATIBILITY).set(schemaValidation);
      return this;
   }

   /**
    * @deprecated since 12.0. Use {@link #allowList()} instead. To be removed in 14.0.
    */
   @Deprecated(forRemoval=true, since = "12.0")
   public WhiteListConfigurationBuilder whiteList() {
      return new WhiteListConfigurationBuilder(allowListBuilder);
   }

   @Override
   public void validate() {
      // No-op, no validation required
   }

   @Override
   public
   SerializationConfiguration create() {
      return new SerializationConfiguration(attributes.protect(), allowListBuilder.create());
   }

   @Override
   public
   SerializationConfigurationBuilder read(SerializationConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      this.allowListBuilder.read(template.allowList(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "SerializationConfigurationBuilder [attributes=" + attributes + "]";
   }
}
