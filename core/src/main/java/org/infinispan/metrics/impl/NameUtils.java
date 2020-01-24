package org.infinispan.metrics.impl;

/**
 * @author anistor@redhat.com
 * @since 10.1.3
 */
final class NameUtils {

   public static String filterIllegalChars(String name) {
      return name.replaceAll("[^\\w]+", "_");
   }

   public static String decamelize(String name) {
      StringBuilder sb = new StringBuilder(name);
      for (int i = 1; i < sb.length(); i++) {
         if (Character.isUpperCase(sb.charAt(i))) {
            sb.insert(i++, '_');
            while (i < sb.length() && Character.isUpperCase(sb.charAt(i))) {
               i++;
            }
         }
      }
      return filterIllegalChars(sb.toString().toLowerCase()).replaceAll("_+", "_");
   }
}
