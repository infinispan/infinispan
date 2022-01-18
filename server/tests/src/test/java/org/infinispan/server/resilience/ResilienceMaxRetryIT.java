package org.infinispan.server.resilience;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.commons.test.Eventually;
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
      // initial_server_list=server0
      RemoteCache<Integer, String> cache = SERVER_TEST.hotrod()
            .withClientConfiguration(new ConfigurationBuilder().maxRetries(0).connectionTimeout(500))
            .withCacheMode(CacheMode.REPL_SYNC)
            .create(0);

      for (int i = 0; i < 100; i++) {
         cache.get(ThreadLocalRandom.current().nextInt());
      }

      InetSocketAddress stoppedServerAddress = SERVERS.getServerDriver().getServerSocket(0, SERVERS.getTestServer().getDefaultPortNumber());
      SERVERS.getServerDriver().stop(0);

      // the client will receive the topology from the server. if the client hit a server that is not working,
      // the client topology won't be updated.
      // we keep iterating until the client topology be: server1 and server2 or the eventually timeout has happened.
      Eventually.eventuallyEquals(2, () -> {
         try {
            cache.get(ThreadLocalRandom.current().nextInt());
         } catch (TransportException e) {
            // assert that the failed server is the one that we killed
            assert e.getMessage().contains(stoppedServerAddress.toString()) : e.getMessage();
         }
         Collection<InetSocketAddress> currentServers = cache.getRemoteCacheManager().getChannelFactory().getServers(cache.getName().getBytes());
         return currentServers.size();
      }, 60000, 1000, TimeUnit.MILLISECONDS);

      // double check that the stopped server was properly removed from the list
      Collection<InetSocketAddress> currentServers = cache.getRemoteCacheManager().getChannelFactory().getServers(cache.getName().getBytes());
      assert currentServers.size() == 2;
      for (InetSocketAddress currentServer : currentServers) {
         if (currentServer.getPort() == stoppedServerAddress.getPort()) {
            throw new IllegalStateException("The removed server should not be present in the list");
         }
      }

      // stop server1 and server2, start server0
      SERVERS.getServerDriver().stop(1);
      SERVERS.getServerDriver().stop(2);
      SERVERS.getServerDriver().restart(0);

      boolean itWorked = false;
      // with 10 requests, one must work
      for (int i = 0; i < 10; i++) {
         try {
            cache.get(ThreadLocalRandom.current().nextInt());
            itWorked = true;
            break;
         } catch (Exception e) {
            // the failure is expected
         }
      }
      assert itWorked;

      // check that the client switched to the initial server list
      currentServers = cache.getRemoteCacheManager().getChannelFactory().getServers(cache.getName().getBytes());
      assert currentServers.size() == 1;
      assert currentServers.iterator().next().getPort() == stoppedServerAddress.getPort();

      // do another operation, it should succeed
      cache.get(ThreadLocalRandom.current().nextInt());
   }
}
