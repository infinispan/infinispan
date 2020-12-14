package org.infinispan.server.logging;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.infinispan.server.logging.log4j.XmlConfiguration;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class XmlConfigurationTest {

   @Test
   public void testXmlConfiguration() throws IOException {
      Path path = getConfigPath().resolve("log4j2.xml");
      try (InputStream is = Files.newInputStream(path)) {
         ConfigurationSource configurationSource = new ConfigurationSource(is, path.toFile());
         LoggerContext context = new LoggerContext("test");
         XmlConfiguration xmlConfiguration = new XmlConfiguration(context, configurationSource);
         xmlConfiguration.initialize();
         assertEquals("InfinispanServerConfig", xmlConfiguration.getName());
      }
   }

   public static Path getConfigPath() {
      return Paths.get(System.getProperty("build.directory"), "test-classes", "configuration");
   }
}
