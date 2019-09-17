package org.infinispan.server.unit.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Properties;

import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.ServerConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.memcached.configuration.MemcachedServerConfiguration;
import org.infinispan.server.network.NetworkAddress;
import org.infinispan.server.router.configuration.SinglePortRouterConfiguration;
import org.infinispan.server.test.TestThreadTrackerRule;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/

public class ServerConfigurationParserTest {

   @Rule
   public TestThreadTrackerRule tracker = new TestThreadTrackerRule();

   @Test
   public void testParser() throws IOException {
      FileLookup fileLookup = FileLookupFactory.newInstance();
      URL url = fileLookup.lookupFileLocation("configuration/" + getClass().getSimpleName() + ".xml", ServerConfigurationParserTest.class.getClassLoader());
      Properties properties = new Properties();
      properties.setProperty(Server.INFINISPAN_SERVER_CONFIG_PATH, System.getProperty("build.directory") + "/test-classes/configuration");
      ParserRegistry registry = new ParserRegistry(this.getClass().getClassLoader(), false, properties);
      ConfigurationBuilderHolder holder = registry.parse(url);
      GlobalConfiguration global = holder.getGlobalConfigurationBuilder().build();
      ServerConfiguration server = global.module(ServerConfiguration.class);

      // Interfaces
      assertEquals(2, server.networkInterfaces().size());
      NetworkAddress defaultInterface = server.networkInterfaces().get("default");
      assertNotNull(defaultInterface);
      assertTrue(defaultInterface.getAddress().isLoopbackAddress());

      // Socket bindings
      assertEquals(5, server.socketBindings().size());
      Assert.assertEquals(11221, server.socketBindings().get("memcached").getPort());
      Assert.assertEquals(12221, server.socketBindings().get("memcached-2").getPort());
      Assert.assertEquals(11222, server.socketBindings().get("default").getPort());
      Assert.assertEquals(11223, server.socketBindings().get("hotrod").getPort());
      Assert.assertEquals(8080, server.socketBindings().get("rest").getPort());

      // Connectors
      List<ProtocolServerConfiguration> connectors = server.endpoints().connectors();
      assertEquals(3, connectors.size());
      assertTrue(connectors.get(0) instanceof HotRodServerConfiguration);
      assertTrue(connectors.get(1) instanceof RestServerConfiguration);
      assertTrue(connectors.get(2) instanceof MemcachedServerConfiguration);

      // Ensure endpoints are bound to the interfaces
      SinglePortRouterConfiguration singlePortRouter = server.endpoints().singlePortRouter();
      assertEquals(server.socketBindings().get("default").getAddress().getAddress().getHostAddress(), singlePortRouter.host());
      assertEquals(server.socketBindings().get("default").getPort(), singlePortRouter.port());
      assertEquals(server.socketBindings().get("memcached").getPort(), server.endpoints().connectors().get(2).port());
   }
}
