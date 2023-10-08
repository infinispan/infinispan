package org.infinispan.configuration.global;

import java.util.List;
import java.util.Objects;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.config.Configuration;

public class SerializationConfiguration {
   public static final AttributeDefinition<Marshaller> MARSHALLER = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.MARSHALLER, null, Marshaller.class)
         .serializer(AttributeSerializer.INSTANCE_CLASS_NAME)
         .immutable().build();
   public static final AttributeDefinition<List<SerializationContextInitializer>> SERIALIZATION_CONTEXT_INITIALIZERS =
         AttributeDefinition.builder("contextInitializers", null, (Class<List<SerializationContextInitializer>>) (Class<?>) List.class)
               .immutable().build();
   public static final AttributeDefinition<Configuration.SchemaValidation> SCHEMA_COMPATIBILITY =
         AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.SCHEMA_COMPATIBILITY, Configuration.SchemaValidation.STRICT, Configuration.SchemaValidation.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SerializationConfiguration.class, MARSHALLER, SERIALIZATION_CONTEXT_INITIALIZERS, SCHEMA_COMPATIBILITY);
   }

   private final Attribute<Marshaller> marshaller;
   private final Attribute<List<SerializationContextInitializer>> contextInitializers;
   private final AttributeSet attributes;
   private final AllowListConfiguration allowListConfig;

   SerializationConfiguration(AttributeSet attributes, AllowListConfiguration allowListConfig) {
      this.attributes = attributes.checkProtection();
      this.marshaller = attributes.attribute(MARSHALLER);
      this.contextInitializers = attributes.attribute(SERIALIZATION_CONTEXT_INITIALIZERS);
      this.allowListConfig = allowListConfig;
   }

   public Marshaller marshaller() {
      return marshaller.get();
   }

   public List<SerializationContextInitializer> contextInitializers() {
      return contextInitializers.get();
   }

   public Configuration.SchemaValidation schemaCompatibilityValidation() {
      return attributes.attribute(SCHEMA_COMPATIBILITY).get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public AllowListConfiguration allowList() {
      return allowListConfig;
   }

   @Deprecated(forRemoval=true, since = "11.0")
   public WhiteListConfiguration whiteList() {
      return new WhiteListConfiguration(allowListConfig);
   }

   @Override
   public String toString() {
      return "SerializationConfiguration{" +
            "attributes=" + attributes +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SerializationConfiguration that = (SerializationConfiguration) o;
      return Objects.equals(attributes, that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes != null ? attributes.hashCode() : 0;
   }
}
