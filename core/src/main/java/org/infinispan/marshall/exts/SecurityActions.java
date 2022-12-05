package org.infinispan.marshall.exts;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

/**
 * SecurityActions for the org.infinispan.marshall.exts package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author William Burns
 * @since 8.2
 */
final class SecurityActions {
   private static Field getDeclaredField(Class<?> c, String fieldName) {
      try {
         return c.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
         return null;
      }
   }

   static Field getField(Class<?> c, String fieldName) {
      Field field = getDeclaredField(c, fieldName);
      if (field != null) {
         field.setAccessible(true);
      }
      return field;
   }

   static <T> Constructor<T> getConstructor(Class<T> c, Class<?>... parameterTypes) {
      try {
         return c.getConstructor(parameterTypes);
      } catch (NoSuchMethodException e) {
         return null;
      }
   }
}
