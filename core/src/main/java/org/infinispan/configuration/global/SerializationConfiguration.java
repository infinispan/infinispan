package org.infinispan.configuration.global;

import java.util.Map;

import org.infinispan.Version;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Marshaller;
import org.jboss.marshalling.ClassResolver;

public class SerializationConfiguration {
   public static final AttributeDefinition<Marshaller> MARSHALLER = AttributeDefinition.builder("marshaller", null, Marshaller.class)
         .immutable().build();
   public static final AttributeDefinition<Short> VERSION = AttributeDefinition.builder("version", Version.getMarshallVersion()).immutable().build();
   public static final AttributeDefinition<ClassResolver> CLASS_RESOLVER = AttributeDefinition.builder("classResolver", null, ClassResolver.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SerializationConfiguration.class, MARSHALLER, VERSION, CLASS_RESOLVER);
   }

   private final Map<Integer, AdvancedExternalizer<?>> advancedExternalizers;
   private final ClassResolver classResolver;
   private final Marshaller marshaller;
   private final short version;
   private final AttributeSet attributes;

   SerializationConfiguration(AttributeSet attributes, Map<Integer, AdvancedExternalizer<?>> advancedExternalizers) {
      this.attributes = attributes.checkProtection();
      this.marshaller = attributes.attribute(MARSHALLER).get();
      this.version = attributes.attribute(VERSION).get();
      this.classResolver = attributes.attribute(CLASS_RESOLVER).get();
      this.advancedExternalizers = advancedExternalizers;
   }

   public Marshaller marshaller() {
      return marshaller;
   }

   public short version() {
      return version;
   }

   public Map<Integer, AdvancedExternalizer<?>> advancedExternalizers() {
      return advancedExternalizers;
   }

   public ClassResolver classResolver() {
      return classResolver;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "SerializationConfiguration [advancedExternalizers=" + advancedExternalizers + ", attributes=" + attributes + "]";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((advancedExternalizers == null) ? 0 : advancedExternalizers.hashCode());
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      SerializationConfiguration other = (SerializationConfiguration) obj;
      if (advancedExternalizers == null) {
         if (other.advancedExternalizers != null)
            return false;
      } else if (!advancedExternalizers.equals(other.advancedExternalizers))
         return false;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

}
