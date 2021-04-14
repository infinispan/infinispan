package org.infinispan.configuration.global;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.CollectionAttributeCopier;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.protostream.SerializationContextInitializer;

public class SerializationConfiguration {
   public static final AttributeDefinition<Marshaller> MARSHALLER = AttributeDefinition.builder("marshaller", null, Marshaller.class)
         .serializer(AttributeSerializer.INSTANCE_CLASS_NAME)
         .immutable().build();
   public static final AttributeDefinition<Map<Integer, AdvancedExternalizer<?>>> ADVANCED_EXTERNALIZERS = AttributeDefinition.builder("advancedExternalizer", null, (Class<Map<Integer, AdvancedExternalizer<?>>>) (Class<?>) Map.class)
         .copier(CollectionAttributeCopier.INSTANCE)
         .initializer(HashMap::new).immutable().build();

   public static final AttributeDefinition<List<SerializationContextInitializer>> SERIALIZATION_CONTEXT_INITIALIZERS =
         AttributeDefinition.builder("contextInitializers", null, (Class<List<SerializationContextInitializer>>) (Class<?>) List.class)
               .immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SerializationConfiguration.class, MARSHALLER, ADVANCED_EXTERNALIZERS, SERIALIZATION_CONTEXT_INITIALIZERS);
   }

   private final Attribute<Map<Integer, AdvancedExternalizer<?>>> advancedExternalizers;
   private final Attribute<Marshaller> marshaller;
   private final Attribute<List<SerializationContextInitializer>> contextInitializers;
   private final AttributeSet attributes;
   private final AllowListConfiguration allowListConfig;

   SerializationConfiguration(AttributeSet attributes, AllowListConfiguration allowListConfig) {
      this.attributes = attributes.checkProtection();
      this.marshaller = attributes.attribute(MARSHALLER);
      this.advancedExternalizers = attributes.attribute(ADVANCED_EXTERNALIZERS);
      this.contextInitializers = attributes.attribute(SERIALIZATION_CONTEXT_INITIALIZERS);
      this.allowListConfig = allowListConfig;
   }

   public Marshaller marshaller() {
      return marshaller.get();
   }

   /**
    * @deprecated since 10.0, {@link AdvancedExternalizer}'s will be removed in a future release.
    */
   @Deprecated
   public Map<Integer, AdvancedExternalizer<?>> advancedExternalizers() {
      return advancedExternalizers.get();
   }

   public List<SerializationContextInitializer> contextInitializers() {
      return contextInitializers.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public AllowListConfiguration allowList() {
      return allowListConfig;
   }

   @Deprecated
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
