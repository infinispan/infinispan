package org.infinispan.rest.framework;

/**
 * @since 10.0
 */
public enum Method {
   GET, PUT, POST, HEAD, DELETE, OPTIONS;

   public static boolean contains(String method) {
      for (Method m : values()) {
         if (m.toString().equals(method)) {
            return true;
         }
      }
      return false;
   }
}
