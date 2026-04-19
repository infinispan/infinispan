package org.infinispan.server;

import java.nio.file.Path;
import java.util.logging.LogManager;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.apache.logging.log4j.core.config.plugins.util.PluginRegistry;
import org.infinispan.commons.logging.log4j.XmlConfigurationFactory;

/**
 * Methods used to bootstrap logging in a JVM environment. This class is here for replacement with Quarkus and if it
 * is updated Infinispan Quarkus must also be updated.
 */
public class BootstrapLogging {

   static void staticInitializer() {
      System.setProperty("log4j2.contextSelector", "org.apache.logging.log4j.core.selector.BasicContextSelector");
      System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");
   }

   static void configureBootLogging() {
      System.setProperty("log4j2.disable.jmx", "true");
      ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();
      builder.setStatusLevel(Level.ERROR);
      builder.setConfigurationName("Bootstrap");
      builder.add(builder.newAppender("Console", "Console")
            .add(builder.newLayout("PatternLayout")
                  .addAttribute("pattern", "%d{HH:mm:ss,SSS} %-5p [%c{1}] %m%n")));
      builder.add(builder.newRootLogger(Level.INFO)
            .add(builder.newAppenderRef("Console")));
      Configurator.initialize(builder.build());
      LogManager logManager = LogManager.getLogManager();
      if (logManager instanceof org.infinispan.server.loader.LogManager) {
         ((org.infinispan.server.loader.LogManager) logManager).setDelegate(new org.apache.logging.log4j.jul.LogManager());
      }
   }

   static void configureLogging(Path loggingFile) {
      System.setProperty("log4j.configurationFactory", XmlConfigurationFactory.class.getName());
      System.setProperty("log4j.configurationFile", loggingFile.toAbsolutePath().toString());
      PluginRegistry.getInstance().clear();
      Configurator.reconfigure();
   }
}
