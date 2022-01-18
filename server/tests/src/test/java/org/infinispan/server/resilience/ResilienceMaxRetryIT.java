package org.infinispan.server.resilience;

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.category.Resilience;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that a client can reset to the initial server list
 * even when it is configured with max_retries=0 and the current server topology
 * contains more than one server.
 */
@Category(Resilience.class)
public class ResilienceMaxRetryIT {

   @ClassRule
   public static InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/ClusteredServerTest.xml")
               .runMode(ServerRunMode.EMBEDDED)
               .numServers(3)
               .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testMaxRetries0() {
      // Start the client with initial_server_list=server0 and max_retries=0
      RemoteCache<Integer, String> cache = SERVER_TEST.hotrod()
            .withClientConfiguration(new ConfigurationBuilder().maxRetries(0).connectionTimeout(500))
            .withCacheMode(CacheMode.REPL_SYNC)
            .create(0);

      // Perform an operation so the client receives a topology update with server0, server1, and server2
      cache.get(ThreadLocalRandom.current().nextInt());

      // Stop server0
      InetSocketAddress stoppedServerAddress = SERVERS.getServerDriver().getServerSocket(0, SERVERS.getTestServer().getDefaultPortNumber());
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
            assert e.getMessage().contains(stoppedServerAddress.toString()) : e.getMessage();
         }
      }

      // Check that the stopped server was properly removed from the list
      Collection<InetSocketAddress> currentServers = cache.getRemoteCacheManager().getChannelFactory().getServers(cacheNameBytes);
      assertEquals(2, currentServers.size());
      for (InetSocketAddress currentServer : currentServers) {
         if (currentServer.equals(stoppedServerAddress)) {
            throw new IllegalStateException(stoppedServerAddress + " should not be present in the list: " + currentServers);
         }
      }

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
         } catch (Exception e) {
            // the failure is expected
         }
      }

      // Check that the client switched to the initial server list
      currentServers = cache.getRemoteCacheManager().getChannelFactory().getServers(cacheNameBytes);
      assertEquals(Collections.singletonList(stoppedServerAddress), currentServers);

      // Do another operation, it should succeed
      cache.get(ThreadLocalRandom.current().nextInt());
   }
}
