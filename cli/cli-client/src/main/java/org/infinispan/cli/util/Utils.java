package org.infinispan.cli.util;

public class Utils {
   /**
    * Returns null if the parameter is null or empty, otherwise it returns it untouched
    */
   public static String nullIfEmpty(String s) {
      if (s != null && s.length() == 0) {
         return null;
      } else {
         return s;
      }
   }
}
