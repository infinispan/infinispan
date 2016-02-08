package org.infinispan.marshall.exts;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

/**
 * SecurityActions for the org.infinispan.marshall.exts package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author William Burns
 * @since 8.2
 */
final class SecurityActions {
   private static final Log log = LogFactory.getLog(SecurityActions.class);

   private static Field getDeclaredField(Class<?> c, String fieldName) {
      try {
         return c.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
         return null;
      }
   }

   static Field getField(Class<?> c, String fieldName) {
      Field field;
      if (System.getSecurityManager() != null) {
         field = AccessController.doPrivileged((PrivilegedAction<Field>) () -> {
            Field f = getDeclaredField(c, fieldName);
            f.setAccessible(true);
            return f;
         });
      } else {
         field = getDeclaredField(c, fieldName);
         field.setAccessible(true);
      }
      return field;
   }
}
