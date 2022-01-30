package org.infinispan.configuration.format;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * Extracts the configuration into flat key-value property structure by reflection.
 *
 * @author Michal Linhard (mlinhard@redhat.com)
 * @since 6.0
 */
public final class PropertyFormatter {

   private static Method plainToString = null;

   static {
      try {
         plainToString = Object.class.getMethod("toString");
      } catch (Exception e) {
         // Ignore
      }
   }

   private final String globalConfigPrefix;
   private final String configPrefix;

   /**
    * Create a new PropertyFormatter instance.
    */
   public PropertyFormatter() {
      this("", "");
   }

   /**
    * Create a new PropertyFormatter instance.
    *
    * @param globalConfigPrefix Prefix used for global configuration property keys.
    * @param configPrefix       Prefix used for cache configuration property keys.
    */
   public PropertyFormatter(String globalConfigPrefix, String configPrefix) {
      this.globalConfigPrefix = globalConfigPrefix;
      this.configPrefix = configPrefix;
   }

   /**
    * Get all public, non-static, non-deprecated methods with 0 arguments (except toString() and other unuuseful ones).
    */
   private static List<Method> getConfigMethods(Class<?> clazz) {
      Class<?> c = clazz;
      List<Method> r = new ArrayList<>();
      while (c != null && c != Object.class) {
         for (Method m : c.getDeclaredMethods()) {
            if (Modifier.isPublic(m.getModifiers())
                  && !Modifier.isStatic(m.getModifiers())
                  && m.getParameterCount() == 0
                  && !m.isAnnotationPresent(Deprecated.class)
                  && !"hashCode".equals(m.getName())
                  && !"toString".equals(m.getName())
                  && !"toProperties".equals(m.getName())) {
               m.setAccessible(true);
               r.add(m);
            }
         }
         c = c.getSuperclass();
      }
      return r;
   }

   private static boolean hasPlainToString(Object obj) {
      Class<?> cls = obj.getClass();
      try {
         if (cls.getMethod("toString") == plainToString) {
            return true;
         }
         String plainToStringValue = cls.getName() + "@" + Integer.toHexString(System.identityHashCode(obj));
         return plainToStringValue.equals(obj.toString());
      } catch (Exception e) {
         return false;
      }
   }

   private static void reflect(Object obj, Properties p, String prefix) {
      try {
         if (obj == null) {
            p.put(prefix, "null");
            return;
         }
         Class<?> cls = obj.getClass();
         if (cls.getName().startsWith("org.infinispan.config") && !cls.isEnum()) {
            for (Method m : getConfigMethods(obj.getClass())) {
               try {
                  String prefixDot = prefix == null || prefix.isEmpty() ? "" : prefix + ".";
                  reflect(m.invoke(obj), p, prefixDot + m.getName());
               } catch (IllegalAccessException e) {
                  // ok
               }
            }
         } else if (Collection.class.isAssignableFrom(cls)) {
            Collection<?> collection = (Collection<?>) obj;
            Iterator<?> iter = collection.iterator();
            for (int i = 0; i < collection.size(); i++) {
               reflect(iter.next(), p, prefix + "[" + i + "]");
            }
         } else if (cls.isArray()) {
            Object[] a = (Object[]) obj;
            for (int i = 0; i < a.length; i++) {
               reflect(a[i], p, prefix + "[" + i + "]");
            }
         } else if (hasPlainToString(obj)) {
            // we have a class that doesn't have a nice toString implementation
            p.put(prefix, cls.getName());
         } else {
            // we have a single value
            p.put(prefix, obj.toString());
         }
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   public Properties format(Configuration configuration) {
      Properties properties = new Properties();
      reflect(configuration, properties, configPrefix);
      return properties;
   }

   public Properties format(GlobalConfiguration configuration) {
      Properties properties = new Properties();
      reflect(configuration, properties, globalConfigPrefix);
      return properties;
   }
}
