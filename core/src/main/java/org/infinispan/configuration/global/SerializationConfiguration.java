package org.infinispan.configuration.global;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.Version;
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
import org.infinispan.configuration.parsing.Element;

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

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.SERIALIZATION.getLocalName());

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SerializationConfiguration.class, MARSHALLER, VERSION, CLASS_RESOLVER, ADVANCED_EXTERNALIZERS);
   }

   private final Attribute<Map<Integer, AdvancedExternalizer<?>>> advancedExternalizers;
   private final Attribute<Object> classResolver;
   private final Attribute<Marshaller> marshaller;
   private final Attribute<Short> version;
   private final AttributeSet attributes;

   SerializationConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.marshaller = attributes.attribute(MARSHALLER);
      this.version = attributes.attribute(VERSION);
      this.classResolver = attributes.attribute(CLASS_RESOLVER);
      this.advancedExternalizers = attributes.attribute(ADVANCED_EXTERNALIZERS);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
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

   public AttributeSet attributes() {
      return attributes;
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

      return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
   }

   @Override
   public int hashCode() {
      return attributes != null ? attributes.hashCode() : 0;
   }
}
