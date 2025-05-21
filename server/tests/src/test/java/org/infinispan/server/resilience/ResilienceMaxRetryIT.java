package org.infinispan.server.resilience;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.category.Resilience;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Test that a client can reset to the initial server list
 * even when it is configured with max_retries=0 and the current server topology
 * contains more than one server.
 */
@Category(Resilience.class)
@Tag("embedded")
public class ResilienceMaxRetryIT {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .runMode(ServerRunMode.EMBEDDED)
               .numServers(3)
               .build();

   @Test
   public void testMaxRetries0() {
      // Start the client with initial_server_list=server0 and max_retries=0
      RemoteCache<Integer, String> cache = SERVERS.hotrod()
            .withClientConfiguration(new ConfigurationBuilder().maxRetries(0).connectionTimeout(500))
            .withCacheMode(CacheMode.REPL_SYNC)
            .create(0);

      // Perform an operation so the client receives a topology update with server0, server1, and server2
      cache.get(ThreadLocalRandom.current().nextInt());

      // Stop server0
      InetSocketAddress serverAddress0 = SERVERS.getServerDriver().getServerSocket(0, SERVERS.getTestServer().getDefaultPortNumber());
      InetSocketAddress serverAddress1 = SERVERS.getServerDriver().getServerSocket(1, SERVERS.getTestServer().getDefaultPortNumber());
      InetSocketAddress serverAddress2 = SERVERS.getServerDriver().getServerSocket(2, SERVERS.getTestServer().getDefaultPortNumber());
      SERVERS.getServerDriver().stop(0);

      // Execute cache operations so the client connects to server1 or server2 and receives a topology update
      // The client keeps trying to connect to failed servers for each new operation,
      // so the number of operations we need depends on which server owns each key
      byte[] cacheNameBytes = cache.getName().getBytes();
      for (int i = 0; i < 10; i++) {
         try {
            cache.get(ThreadLocalRandom.current().nextInt());
            break;
         } catch (TransportException e) {
            // Assert that the failed server is the one that we killed
            assertTrue(e.getMessage().contains(serverAddress0.toString()), serverAddress0 + " not found in " + e.getMessage());
         }
      }

      // Check that the stopped server was properly removed from the list
      Collection<InetSocketAddress> currentServers = cache.getRemoteCacheManager().getChannelFactory().getServers(cacheNameBytes);
      assertEquals(new HashSet<>(asList(serverAddress1, serverAddress2)), new HashSet<>(resolveAddresses(currentServers)));

      // Stop server1 and server2, start server0
      SERVERS.getServerDriver().stop(1);
      SERVERS.getServerDriver().stop(2);
      SERVERS.getServerDriver().restart(0);

      // Execute cache operations until the client resets to the initial server list (server0)
      // The reset happens when all current servers (server1 and server2) are marked failed
      // But the client keeps trying to connect to failed servers for each new operation,
      // so the number of operations we need depends on which server owns each key
      for (int i = 0; i < 10; i++) {
         try {
            cache.get(ThreadLocalRandom.current().nextInt());
            break;
         } catch (TransportException e) {
            // Expected to fail to connect to server1 or server2, not server0
            assertTrue(e.getMessage().contains(serverAddress1.toString()) || e.getMessage().contains(serverAddress2.toString()), e.getMessage());
         }
      }

      // Check that the client switched to the initial server list
      currentServers = cache.getRemoteCacheManager().getChannelFactory().getServers(cacheNameBytes);
      assertEquals(singletonList(serverAddress0), resolveAddresses(currentServers));

      // Do another operation, it should succeed
      cache.get(ThreadLocalRandom.current().nextInt());
   }

   /**
    * ChannelFactory keeps the addresses unresolved, so we must convert the addresses to unresolved if we want them to match
    */
   private Collection<InetSocketAddress> resolveAddresses(Collection<InetSocketAddress> serverAddresses) {
      List<InetSocketAddress> list = new ArrayList<>(serverAddresses.size());
      for (InetSocketAddress serverAddress : serverAddresses) {
         list.add(new InetSocketAddress(serverAddress.getHostString(), serverAddress.getPort()));
      }
      return list;
   }
}
