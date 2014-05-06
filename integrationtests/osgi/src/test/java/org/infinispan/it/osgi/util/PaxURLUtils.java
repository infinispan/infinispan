package org.infinispan.it.osgi.util;

public class PaxURLUtils {
   public static final String PROP_PAX_URL_LOCAL_REPO = "org.ops4j.pax.url.mvn.localRepository";

   private static final String JAVA_URL_HANDLERS_PROPERTY = "java.protocol.handler.pkgs";
   private static final String PAX_URL_PACKAGE = "org.ops4j.pax.url";

   public static void registerURLHandlers() {
      String protocolHandlers = System.getProperty(JAVA_URL_HANDLERS_PROPERTY);

      if (protocolHandlers == null) {
         System.setProperty(JAVA_URL_HANDLERS_PROPERTY, PAX_URL_PACKAGE);
      } else if (!protocolHandlers.contains(PAX_URL_PACKAGE)) {
         System.setProperty(JAVA_URL_HANDLERS_PROPERTY, String.format("%s|%s",protocolHandlers, PAX_URL_PACKAGE));
      }
   }

   public static void configureLocalMavenRepo() {
      String localRepo = null;
      try {
         localRepo = MavenUtils.getLocalRepository();
      } catch (Exception ex) {
      }
      if (localRepo == null) {
         return;
      }
      System.setProperty(PROP_PAX_URL_LOCAL_REPO, localRepo);
   }
}
