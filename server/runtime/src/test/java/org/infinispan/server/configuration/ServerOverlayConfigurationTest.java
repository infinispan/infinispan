package org.infinispan.server.configuration;

import static org.infinispan.server.configuration.ServerConfigurationParserTest.getConfigPath;
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
      Server server = new Server(getConfigPath().toFile(), Arrays.asList(Paths.get("Base.xml"), Paths.get("Overlay.yml")), properties);
      ConfigurationBuilderHolder holder = server.getConfigurationBuilderHolder();
      assertTrue(holder.getNamedConfigurationBuilders().containsKey("overlay"));
      GlobalConfiguration global = holder.getGlobalConfigurationBuilder().build();
      assertNotNull(global.module(ServerConfiguration.class));
   }
}
