package org.infinispan.server.integration.enricher;

public class ArquillianSupport {

   public static boolean isClientMode() {
      return hasTheClass() == true;
   }

   public static boolean isServerMode() {
      return hasTheClass() == false;
   }

   private static boolean hasTheClass() {
      boolean hasTheClass = false;
      try {
         Class.forName("org.infinispan.server.integration.InstrumentArquillianContainer");
         hasTheClass = true;
      } catch (ClassNotFoundException e) {
         // no problem
      }
      return hasTheClass;
   }
}
