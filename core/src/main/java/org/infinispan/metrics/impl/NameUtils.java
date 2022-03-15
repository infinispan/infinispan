package org.infinispan.metrics.impl;

/**
 * @author anistor@redhat.com
 * @since 10.1.3
 */
final class NameUtils {

   /**
    * Replace illegal metric name chars with underscores.
    */
   public static String filterIllegalChars(String name) {
      return name.replaceAll("[^\\w]+", "_");
   }

   /**
    * Transform a camel-cased name to snake-case, because Micrometer metrics loves underscores. Eventual sequences of
    * multiple underscores are replaced with a single underscore. Illegal characters are also replaced with underscore.
    */
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
