package org.infinispan.server;

import java.nio.file.Path;
import java.util.logging.LogManager;

import org.infinispan.server.logging.log4j.XmlConfigurationFactory;

/**
 * Methods used to bootstrap logging in a JVM environment. This class is here for replacement with Quarkus and if it
 * is updated Infinispan Quarkus must also be updated.b
 */
public class BootstrapLogging {

   static void staticInitializer() {
      System.setProperty("log4j2.contextSelector", "org.apache.logging.log4j.core.selector.BasicContextSelector");
      System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");
   }

   static void configureLogging(Path loggingFile) {
      System.setProperty("log4j.configurationFactory", XmlConfigurationFactory.class.getName());
      System.setProperty("log4j.configurationFile", loggingFile.toAbsolutePath().toString());
      LogManager logManager = LogManager.getLogManager();
      if (logManager instanceof org.infinispan.server.loader.LogManager) {
         ((org.infinispan.server.loader.LogManager) logManager).setDelegate(new org.apache.logging.log4j.jul.LogManager());
      }
   }
}
