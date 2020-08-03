package org.infinispan.server.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.infinispan.commons.test.junit.JUnitThreadTrackerRule;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.server.Server;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.memcached.configuration.MemcachedServerConfiguration;
import org.infinispan.server.network.NetworkAddress;
import org.infinispan.server.network.SocketBinding;
import org.infinispan.server.router.configuration.SinglePortRouterConfiguration;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/

public class ServerConfigurationParserTest {

   @ClassRule
   public static final JUnitThreadTrackerRule tracker = new JUnitThreadTrackerRule();

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
      Map<String, SocketBinding> socketBindings = server.socketBindings();
      assertEquals(5, socketBindings.size());
      assertEquals(11221, socketBindings.get("memcached").getPort());
      assertEquals(12221, socketBindings.get("memcached-2").getPort());
      assertEquals(11222, socketBindings.get("default").getPort());
      assertEquals(11223, socketBindings.get("hotrod").getPort());
      assertEquals(8080, socketBindings.get("rest").getPort());

      // Data Sources
      Map<String, DataSourceConfiguration> dataSources = server.dataSources();
      assertEquals(1, dataSources.size());
      DataSourceConfiguration dataSource = dataSources.get("database");
      assertEquals("jdbc/database", dataSource.jndiName());
      assertEquals("jdbc:h2:tcp://${org.infinispan.test.host.address}:1521/test", dataSource.url());
      assertEquals("test", dataSource.username());
      assertEquals("test", dataSource.password());
      assertEquals("SELECT 1", dataSource.initialSql());
      assertEquals("org.h2.Driver", dataSource.driver());
      assertEquals(10, dataSource.maxSize());
      assertEquals(1, dataSource.minSize());
      assertEquals(1, dataSource.initialSize());
      assertEquals(1, dataSource.connectionProperties().size());
      assertEquals("somevalue", dataSource.connectionProperties().get("someproperty"));

      // Connectors
      List<ProtocolServerConfiguration> connectors = server.endpoints().endpoints().get(0).connectors();
      assertEquals(3, connectors.size());
      assertTrue(connectors.get(0) instanceof HotRodServerConfiguration);
      assertTrue(connectors.get(1) instanceof RestServerConfiguration);
      assertTrue(connectors.get(2) instanceof MemcachedServerConfiguration);

      // Ensure endpoints are bound to the interfaces
      SinglePortRouterConfiguration singlePortRouter = server.endpoints().endpoints().get(0).singlePortRouter();
      assertEquals(socketBindings.get("default").getAddress().getAddress().getHostAddress(), singlePortRouter.host());
      assertEquals(socketBindings.get("default").getPort(), singlePortRouter.port());
      assertEquals(socketBindings.get("memcached").getPort(), server.endpoints().endpoints().get(0).connectors().get(2).port());
   }
}
