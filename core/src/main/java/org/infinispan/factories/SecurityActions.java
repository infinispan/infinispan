package org.infinispan.factories;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Properties;
import java.util.Map.Entry;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * SecurityActions for the org.infinispan.factories package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
final class SecurityActions {
   private static final Log log = LogFactory.getLog(SecurityActions.class);

   private static Field findFieldRecursively(Class<?> c, String fieldName) {
      Field f = null;
      try {
         f = c.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
         if (!c.equals(Object.class)) f = findFieldRecursively(c.getSuperclass(), fieldName);
      }
      return f;
   }

   static void setValue(Object instance, String fieldName, Object value) {
      try {
         final Field f = findFieldRecursively(instance.getClass(), fieldName);
         if (f == null)
            throw new NoSuchMethodException("Cannot find field " + fieldName + " on " + instance.getClass() + " or superclasses");
         if (System.getSecurityManager() != null) {
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
               @Override
               public Void run() {
                  f.setAccessible(true);
                  return null;
               }
            });
         } else {
            f.setAccessible(true);
         }
         f.set(instance, value);
      } catch (Exception e) {
         log.unableToSetValue(e);
      }
   }


   static void applyProperties(Object o, Properties p) {
      for(Entry<Object, Object> entry : p.entrySet()) {
         setValue(o, (String) entry.getKey(), entry.getValue());
      }
   }
}
