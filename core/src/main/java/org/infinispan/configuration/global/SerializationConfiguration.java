package org.infinispan.configuration.global;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.CollectionAttributeCopier;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.protostream.SerializationContextInitializer;

public class SerializationConfiguration implements ConfigurationInfo {
   public static final AttributeDefinition<Marshaller> MARSHALLER = AttributeDefinition.builder("marshaller", null, Marshaller.class)
         .immutable().build();
   public static final AttributeDefinition<Short> VERSION = AttributeDefinition.builder("version", Version.getMarshallVersion())
         .serializer(new AttributeSerializer<Short, ConfigurationInfo, ConfigurationBuilderInfo>() {
            @Override
            public Object getSerializationValue(Attribute<Short> attribute, ConfigurationInfo configurationElement) {
               return Version.decodeVersion(attribute.get());
            }
         })
         .immutable().build();
   public static final AttributeDefinition<Object> CLASS_RESOLVER = AttributeDefinition.builder("classResolver", null, Object.class).immutable().build();
   public static final AttributeDefinition<Map<Integer, AdvancedExternalizer<?>>> ADVANCED_EXTERNALIZERS = AttributeDefinition.builder("advancedExternalizer", null, (Class<Map<Integer, AdvancedExternalizer<?>>>) (Class<?>) Map.class)
         .copier(CollectionAttributeCopier.INSTANCE)
         .initializer(HashMap::new).immutable().build();

   public static final AttributeDefinition<Collection<SerializationContextInitializer>> SERIALIZATION_CONTEXT_INITIALIZERS =
         AttributeDefinition.builder("contextInitializers", null, (Class<Collection<SerializationContextInitializer>>) (Class<?>) Collection.class).immutable().build();

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.SERIALIZATION.getLocalName());

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SerializationConfiguration.class, MARSHALLER, VERSION, CLASS_RESOLVER, ADVANCED_EXTERNALIZERS, SERIALIZATION_CONTEXT_INITIALIZERS);
   }

   private final Attribute<Map<Integer, AdvancedExternalizer<?>>> advancedExternalizers;
   private final Attribute<Object> classResolver;
   private final Attribute<Marshaller> marshaller;
   private final Attribute<Short> version;
   private final Attribute<Collection<SerializationContextInitializer>> contextInitializers;
   private final AttributeSet attributes;
   private final WhiteListConfiguration whiteListConfig;
   private final List<ConfigurationInfo> subElements;

   SerializationConfiguration(AttributeSet attributes, WhiteListConfiguration whiteListConfig) {
      this.attributes = attributes.checkProtection();
      this.marshaller = attributes.attribute(MARSHALLER);
      this.version = attributes.attribute(VERSION);
      this.classResolver = attributes.attribute(CLASS_RESOLVER);
      this.advancedExternalizers = attributes.attribute(ADVANCED_EXTERNALIZERS);
      this.contextInitializers = attributes.attribute(SERIALIZATION_CONTEXT_INITIALIZERS);
      this.whiteListConfig = whiteListConfig;
      this.subElements = Collections.singletonList(whiteListConfig);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElements;
   }

   public Marshaller marshaller() {
      return marshaller.get();
   }

   public short version() {
      return version.get();
   }

   public Map<Integer, AdvancedExternalizer<?>> advancedExternalizers() {
      return advancedExternalizers.get();
   }

   @Deprecated
   public Object classResolver() {
      return classResolver.get();
   }

   public Collection<SerializationContextInitializer> contextInitializers() {
      return contextInitializers.get();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public WhiteListConfiguration whiteList() {
      return whiteListConfig;
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
