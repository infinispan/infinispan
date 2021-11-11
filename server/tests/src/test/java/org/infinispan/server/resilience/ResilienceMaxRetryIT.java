package org.infinispan.server.resilience;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.UUID;

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

@Category(Resilience.class)
public class ResilienceMaxRetryIT {

   @ClassRule
   public static InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/ClusteredServerTest.xml")
               .runMode(ServerRunMode.CONTAINER)
               .numServers(3)
               .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testMaxRetries0() {
      // 1 -> configure max-retries="0" and initial_server_list=localhost:port1
      // 2 -> start 3 servers, localhost:port1, localhost:port2 and localhost:port3
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.maxRetries(0);
      RemoteCache<String, String> cache = SERVER_TEST.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.REPL_SYNC).create(0);

      // 3 -> start the client and do some operations
      for (int i = 0; i < 100; i++) {
         String random = UUID.randomUUID().toString();
         cache.put(random, random);
      }

      // 4 -> kill localhost:port1
      InetAddress killedServer = SERVERS.getServerDriver().getServerAddress(0);
      SERVERS.getServerDriver().kill(0);

      // 5 -> do more operations, check that it's only connected to port2 and port3
      // thinking one failure would be enough, but the topology is actually updated only when the client connects to one of the live servers
      // you could have the first 2 connections to port1 and they'd both fail
      // more than 2 is highly unlikely, but it might happen because the probability is not 0
      for (int i = 0; i < 100; i++) {
         String random = UUID.randomUUID().toString();
         try {
            cache.put(random, random);
         } catch (TransportException e) {
            // if you do 10 operations after you kill port1, the first 0 or 1 or 2 operations might fail.
            // But if the 3rd operation is successful, then operations 4-10 should be successful as well
            assert i == 0 || i == 1 || i == 2;
            // assert that the failed server is the one that we killed
            assert e.getMessage().contains(killedServer.toString());
         }
      }

      // double check that the killed server was properly removed from the list
      Collection<InetSocketAddress> currentServers = cache.getRemoteCacheManager().getChannelFactory().getServers(cache.getName().getBytes());
      assert currentServers.size() == 2;
      for (InetSocketAddress currentServer : currentServers) {
         if (currentServer.getHostString().equals(killedServer.getHostAddress())) {
            throw new IllegalStateException("The removed server should not be present in the list");
         }
      }

      // 6 -> kill port2 and port3, start port1
      SERVERS.getServerDriver().kill(1);
      SERVERS.getServerDriver().kill(2);
      SERVERS.getServerDriver().restart(0);

      // 7 -> do one operation, it should fail
      // 8 -> do one more operation, it should also fail
      // switching might require more than 2 failed requests
      // because we track the failed servers globally when we decide whether to switch, but we don't use that information to decide where a request should go
      // if both requests might hit the same server, the client will think the other server is still alive and won't switch yet
      boolean itWorked = false;
      // with 10 requests, one must work
      for (int i = 0; i < 10; i++) {
         String random = UUID.randomUUID().toString();
         try {
            cache.put(random, random);
            itWorked = true;
            break;
         } catch (Exception e) {
            // the failure is expected
         }
      }
      assert itWorked;

      // 9 -> check that the client switched to the initial server list
      currentServers = cache.getRemoteCacheManager().getChannelFactory().getServers(cache.getName().getBytes());
      assert currentServers.iterator().next().getHostString().equals(killedServer.getHostAddress());

      // 10 -> do another operation, it should succeed
      String random = UUID.randomUUID().toString();
      cache.put(random, random);
   }
}
