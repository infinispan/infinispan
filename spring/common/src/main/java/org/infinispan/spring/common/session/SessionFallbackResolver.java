package org.infinispan.spring.common.session;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.spring.common.marshalling.JacksonSessionFallbackMarshaller;
import org.infinispan.spring.common.marshalling.SpringJavaSerializationMarshaller;

/**
 * Resolves the fallback marshaller for session attributes that cannot be
 * serialized using ProtoStream.
 * <p>
 * Supports three modes:
 * <ul>
 *   <li>{@code null}, empty, or {@code "java"} - uses Java serialization with Spring-specific allow patterns</li>
 *   <li>{@code "jackson"} - uses Jackson with Spring Security modules (requires spring-security-jackson2 on classpath)</li>
 *   <li>Fully qualified class name - instantiates a custom marshaller via reflection</li>
 * </ul>
 * <p>
 * <strong>Usage Note:</strong> This utility class is available for configurable session fallback
 * serialization but is not automatically wired into the Spring Boot auto-configuration layer
 * in the current implementation. The default session fallback remains {@link SpringJavaSerializationMarshaller}.
 * This class can be used by custom configurations or future auto-configuration enhancements.
 *
 * @since 16.3
 */
public final class SessionFallbackResolver {

   private SessionFallbackResolver() {}

   /**
    * Resolves a marshaller based on the given value.
    *
    * @param value the fallback serializer name or FQCN
    * @param classLoader the class loader to use for loading classes
    * @return the resolved marshaller
    * @throws IllegalStateException if "jackson" is requested but spring-security-jackson2 is not on the classpath
    * @throws IllegalArgumentException if a custom class name cannot be instantiated
    */
   public static Marshaller resolve(String value, ClassLoader classLoader) {
      if (value == null || value.isEmpty() || "java".equals(value)) {
         return new SpringJavaSerializationMarshaller();
      }

      if ("jackson".equals(value)) {
         return createJacksonMarshaller(classLoader);
      }

      return instantiateByClassName(value);
   }

   private static Marshaller createJacksonMarshaller(ClassLoader classLoader) {
      try {
         Class.forName("org.springframework.security.jackson2.SecurityJackson2Modules", false, classLoader);
      } catch (ClassNotFoundException e) {
         throw new IllegalStateException(
               "Session fallback serializer 'jackson' requires spring-security-jackson2 on the classpath. " +
               "Add the dependency or use 'java' as the fallback.", e);
      }
      return new JacksonSessionFallbackMarshaller(classLoader);
   }

   private static Marshaller instantiateByClassName(String className) {
      try {
         Class<?> clazz = Class.forName(className);
         return (Marshaller) clazz.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
         throw new IllegalArgumentException(
               "Cannot instantiate session fallback serializer class '" + className + "'.", e);
      }
   }
}
