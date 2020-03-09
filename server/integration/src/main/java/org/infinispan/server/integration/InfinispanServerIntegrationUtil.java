package org.infinispan.server.integration;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;

public class InfinispanServerIntegrationUtil {

   public static void setFieldValue(Field field, Object target, Object value) {
      setAccessible(field);
      try {
         field.set(target, value);
      } catch (Exception ex) {
         throw new RuntimeException("Could not set static RemoteCacheManager field for test class" + field.getDeclaringClass().getName(), ex);
      }
   }

   public static InfinispanServerTestMethodRule getInfinispanServerTestMethodRule(Field field, Object target) {
      setAccessible(field);
      try {
         return (InfinispanServerTestMethodRule) field.get(target);
      } catch (Exception ex) {
         throw new RuntimeException("Could not set static RemoteCacheManager field for test class" + field.getDeclaringClass().getName(), ex);
      }
   }

   private static void setAccessible(Field field) {
      AccessController.doPrivileged(new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            field.setAccessible(true);
            return null;
         }
      });
   }
}
