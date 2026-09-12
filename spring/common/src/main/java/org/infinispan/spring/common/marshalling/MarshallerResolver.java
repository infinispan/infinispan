package org.infinispan.spring.common.marshalling;

import static org.infinispan.spring.common.marshalling.MarshallerResolver.MarshallerAlias.JAVA;
import static org.infinispan.spring.common.marshalling.MarshallerResolver.MarshallerAlias.PROTOSTREAM;

import java.lang.invoke.MethodHandles;
import java.util.logging.Logger;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;

/**
 * Resolves marshaller instances based on configuration shortcuts or FQCN.
 */
public final class MarshallerResolver {

   public enum MarshallerAlias {
      JAVA, PROTOSTREAM
   }

   private static final Logger logger = Logger.getLogger(MethodHandles.lookup().lookupClass().getName());

   private MarshallerResolver() {}

   /**
    * Resolves a marshaller based on the given value.
    *
    * @param value the marshaller value: null/empty (defaults to Java Serialization), "java" (Java Serialization),
    *              "protostream" (ProtoStream), or a fully qualified class name
    * @return the resolved marshaller instance
    */
   public static Marshaller resolve(String value) {
      if (value == null || value.isEmpty()) {
         logger.warning("ISPN-17959: Infinispan Spring Boot is defaulting to Java Serialization " +
               "for cache marshalling. JavaSerializationMarshaller class is deprecated and will be removed in a " +
               "future version. To use the recommended ProtoStream marshaller, add @Proto annotations " +
               "to your cached types and set infinispan.remote.marshaller=protostream (or " +
               "infinispan.embedded.marshaller=protostream). To continue using Java Serialization and " +
               "suppress this warning, set the marshaller property to 'java'.");
         return new SpringJavaSerializationMarshaller();
      }

      return switch (value) {
         case JAVA -> new SpringJavaSerializationMarshaller();
         case PROTOSTREAM -> new ProtoStreamMarshaller();
         default -> instantiateByClassName(value);
      };
   }

   private static Marshaller instantiateByClassName(String className) {
      try {
         Class<?> clazz = Class.forName(className);
         return (Marshaller) clazz.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
         throw new IllegalArgumentException(
               "Cannot instantiate marshaller class '" + className + "'. Ensure the class " +
               "implements org.infinispan.commons.marshall.Marshaller and has a no-arg constructor.", e);
      }
   }
}
