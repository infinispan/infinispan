package org.infinispan.server.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;

import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.memcached.configuration.MemcachedServerConfiguration;
import org.infinispan.server.server.configuration.ServerConfiguration;
import org.infinispan.server.server.network.NetworkAddress;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/

public class ServerConfigurationParserTest {

   @Test
   public void testParser() throws IOException {
      FileLookup fileLookup = FileLookupFactory.newInstance();
      try (InputStream is = fileLookup.lookupFile("infinispan.xml", ServerConfigurationParserTest.class.getClassLoader())) {
         ParserRegistry registry = new ParserRegistry();
         ConfigurationBuilderHolder holder = registry.parse(is);
         GlobalConfiguration global = holder.getGlobalConfigurationBuilder().build();
         ServerConfiguration server = global.module(ServerConfiguration.class);

         // Interfaces
         assertEquals(2, server.networkInterfaces().size());
         NetworkAddress publicInterface = server.networkInterfaces().get("public");
         assertNotNull(publicInterface);
         assertTrue(publicInterface.getAddress().isLoopbackAddress());
         NetworkAddress adminInterface = server.networkInterfaces().get("admin");
         assertNotNull(adminInterface);
         assertTrue(adminInterface.getAddress().isLoopbackAddress());


         // Socket bindings
         assertEquals(4, server.socketBindings().size());
         assertEquals(11222, server.socketBindings().get("hotrod").getPort());
         assertEquals(11221, server.socketBindings().get("memcached").getPort());
         assertEquals(8080, server.socketBindings().get("rest").getPort());

         // Endpoints
         assertEquals(3, server.endpoints().size());
         assertTrue(server.endpoints().get(0) instanceof HotRodServerConfiguration);
         assertTrue(server.endpoints().get(1) instanceof MemcachedServerConfiguration);
         assertTrue(server.endpoints().get(2) instanceof RestServerConfiguration);

         // Ensure endpoints are bound to the interfaces
         assertEquals(publicInterface.getAddress().getHostAddress(), server.endpoints().get(0).host());
      }
   }
}
