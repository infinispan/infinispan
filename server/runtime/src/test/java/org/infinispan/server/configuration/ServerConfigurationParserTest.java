package org.infinispan.server.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.server.Server;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.memcached.configuration.MemcachedServerConfiguration;
import org.infinispan.server.network.NetworkAddress;
import org.infinispan.test.fwk.TestResourceTracker;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/

public class ServerConfigurationParserTest {

   @BeforeClass
   public static void before() {
      TestResourceTracker.testStarted(ServerConfigurationParserTest.class.getName());
   }

   @AfterClass
   public static void after() {
      TestResourceTracker.testFinished(ServerConfigurationParserTest.class.getName());
   }


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
      assertEquals(2, server.socketBindings().size());
      Assert.assertEquals(11222, server.socketBindings().get("default").getPort());
      Assert.assertEquals(11221, server.socketBindings().get("memcached").getPort());

      // Connectors
      assertEquals(3, server.connectors().size());
      assertTrue(server.connectors().get(0) instanceof HotRodServerConfiguration);
      assertTrue(server.connectors().get(1) instanceof RestServerConfiguration);
      assertTrue(server.connectors().get(2) instanceof MemcachedServerConfiguration);

      // Ensure endpoints are bound to the interfaces
      assertEquals(server.socketBindings().get("default").getAddress().getAddress().getHostAddress(), server.endpoint().host());
      assertEquals(server.socketBindings().get("default").getPort(), server.endpoint().port());
      assertEquals(server.socketBindings().get("memcached").getPort(), server.connectors().get(2).port());
   }
}
