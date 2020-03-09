package org.infinispan.server.integration;

public enum ArquillianServerType {

   NONE, TOMCAT9, WILDFLY18;

   public static ArquillianServerType current() {
      ArquillianServerType type = ArquillianServerType.NONE;
      final String launchType = System.getProperty("infinispan.server.integration.launch");
      if (launchType != null && !launchType.isEmpty()) {
         type = ArquillianServerType.valueOf(launchType.toUpperCase());
      }
      return type;
   }
}
