package org.infinispan.commons.ssl;

public class SslContextName {

   private static String DEFAULT = "";
   private static String OPENSSL = "openssl";

   public static Object[][] PROVIDER;
   static {
      if (Boolean.parseBoolean(System.getProperty("org.infinispan.openssl", "true"))) {
         PROVIDER = new Object[][] {
               {DEFAULT},
               {OPENSSL}
         };
      } else {
         PROVIDER = new Object[][] {
               {DEFAULT}
         };
      }
   }
}
