package org.infinispan.server.configuration;

import static org.infinispan.server.configuration.ServerConfigurationParserTest.getConfigPath;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.server.Server;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class ServerOverlayConfigurationTest {

   @Test
   public void testOverlay() {
      Properties properties = new Properties();
      properties.put(Server.INFINISPAN_SERVER_CONFIG_PATH, getConfigPath().toString());
      properties.put(Server.INFINISPAN_SERVER_HOME_PATH, getConfigPath().toString());
      try (Server server = new Server(getConfigPath().toFile(), Arrays.asList(Paths.get("Base.xml"), Paths.get("Overlay.yml")), properties)) {
         ConfigurationBuilderHolder holder = server.getConfigurationBuilderHolder();
         assertTrue(holder.getNamedConfigurationBuilders().containsKey("overlay"));
         GlobalConfiguration global = holder.getGlobalConfigurationBuilder().build();
         assertNotNull(global.module(ServerConfiguration.class));
      }
   }

   @Test
   public void testOverlayTwice() {
      Properties properties = new Properties();
      properties.put(Server.INFINISPAN_SERVER_CONFIG_PATH, getConfigPath().toString());
      try (Server server = new Server(getConfigPath().toFile(),
            Arrays.asList(Paths.get("Base.xml"),
                  Paths.get("Overlay.yml"),
                  Paths.get("Overlay-AsyncReplicatedCache.yml")),
            properties)) {
         ConfigurationBuilderHolder holder = server.getConfigurationBuilderHolder();
         assertTrue(holder.getNamedConfigurationBuilders().containsKey("secondary-cache"));
         assertFalse(holder.getGlobalConfigurationBuilder().cacheContainer().statistics());
         assertFalse(holder.getGlobalConfigurationBuilder().cacheContainer().jmx().enabled());
         GlobalConfiguration global = holder.getGlobalConfigurationBuilder().build();
         assertNotNull(global.module(ServerConfiguration.class));
      }
   }

   @Test
   public void testOverlayManyConfigurations() {
      Properties properties = new Properties();
      properties.put(Server.INFINISPAN_SERVER_CONFIG_PATH, getConfigPath().toString());
      try (Server server = new Server(getConfigPath().toFile(),
            Arrays.asList(Paths.get("Base.xml"),
                  Paths.get("Overlay.yml"),
                  Paths.get("Overlay-AsyncReplicatedCache.yml"),
                  Paths.get("OverlayJmx.yml"),
                  Paths.get("OverlayMetrics.xml")),
            properties)) {
         ConfigurationBuilderHolder holder = server.getConfigurationBuilderHolder();
         assertTrue(holder.getNamedConfigurationBuilders().containsKey("overlay"));
         assertTrue(holder.getNamedConfigurationBuilders().containsKey("secondary-cache"));
         GlobalConfiguration global = holder.getGlobalConfigurationBuilder().build();
         assertNotNull(global.module(ServerConfiguration.class));
      }
   }

   @Test
   public void testUnorderedOverlay() {
      Properties properties = new Properties();
      properties.put(Server.INFINISPAN_SERVER_CONFIG_PATH, getConfigPath().toString());
      try (Server server = new Server(getConfigPath().toFile(),
            Arrays.asList(
                  Paths.get("OverlayMetrics.xml"),
                  Paths.get("Overlay-AsyncReplicatedCache.yml"),
                  Paths.get("OverlayJmx.yml"),
                  Paths.get("Overlay.yml"),
                  Paths.get("Base.xml")),
            properties)) {
         ConfigurationBuilderHolder holder = server.getConfigurationBuilderHolder();
         assertTrue(holder.getNamedConfigurationBuilders().containsKey("overlay"));
         assertTrue(holder.getNamedConfigurationBuilders().containsKey("secondary-cache"));
         assertTrue(holder.getGlobalConfigurationBuilder().cacheContainer().statistics());
         assertTrue(holder.getGlobalConfigurationBuilder().cacheContainer().jmx().enabled());
         GlobalConfiguration global = holder.getGlobalConfigurationBuilder().build();
         assertNotNull(global.module(ServerConfiguration.class));
      }
   }
}
