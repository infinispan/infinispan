package org.infinispan.configuration.global;

import java.util.Map;

import org.infinispan.Version;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.protostream.SerializationContextInitializer;

public class SerializationConfiguration {
   public static final AttributeDefinition<Marshaller> MARSHALLER = AttributeDefinition.builder("marshaller", null, Marshaller.class)
         .immutable().build();
   public static final AttributeDefinition<Short> VERSION = AttributeDefinition.builder("version", Version.getMarshallVersion()).immutable().build();
   @Deprecated
   public static final AttributeDefinition<Object> CLASS_RESOLVER = AttributeDefinition.builder("classResolver", null, Object.class).immutable().build();

   public static final AttributeDefinition<SerializationContextInitializer> CONTEXT_INITIALIZER = AttributeDefinition.builder("contextInitializer", null, SerializationContextInitializer.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SerializationConfiguration.class, MARSHALLER, VERSION, CLASS_RESOLVER, CONTEXT_INITIALIZER);
   }

   private final Map<Integer, AdvancedExternalizer<?>> advancedExternalizers;
   private final Attribute<Object> classResolver;
   private final Attribute<Marshaller> marshaller;
   private final Attribute<Short> version;
   private final Attribute<SerializationContextInitializer> contextInitializer;
   private final AttributeSet attributes;

   SerializationConfiguration(AttributeSet attributes, Map<Integer, AdvancedExternalizer<?>> advancedExternalizers) {
      this.attributes = attributes.checkProtection();
      this.marshaller = attributes.attribute(MARSHALLER);
      this.version = attributes.attribute(VERSION);
      this.classResolver = attributes.attribute(CLASS_RESOLVER);
      this.contextInitializer = attributes.attribute(CONTEXT_INITIALIZER);
      this.advancedExternalizers = advancedExternalizers;
   }

   public Marshaller marshaller() {
      return marshaller.get();
   }

   public short version() {
      return version.get();
   }

   public Map<Integer, AdvancedExternalizer<?>> advancedExternalizers() {
      return advancedExternalizers;
   }

   /**
    * @throws {@link ClassNotFoundException} if the infinispan-jboss-marshalling or jboss-marshalling jar are not on the classpath
    */
   @Deprecated
   public Object classResolver() {
      return classResolver.get();
   }

   public SerializationContextInitializer contextInitializer() {
      return contextInitializer.get();
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
