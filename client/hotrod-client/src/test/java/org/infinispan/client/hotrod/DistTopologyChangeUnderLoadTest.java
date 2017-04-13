package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.getLoadBalancer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.net.SocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.DistTopologyChangeUnderLoadTest")
public class DistTopologyChangeUnderLoadTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(1, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(2);
      return hotRodCacheConfiguration(builder);
   }

   @Override
   protected int maxRetries() {
      return 1;
   }

   public void testPutsSucceedWhileTopologyChanges() throws Exception {
      RemoteCache<Integer, String> remote = client(0).getCache();
      remote.put(1, "v1");
      assertEquals("v1", remote.get(1));

      PutHammer putHammer = new PutHammer();
      Future<Void> putHammerFuture = fork(putHammer);

      HotRodServer newServer = addHotRodServer(getCacheConfiguration());

      // Wait for 2 seconds
      TestingUtil.sleepThread(2000);

      HotRodClientTestingUtil.killServers(newServer);
      TestingUtil.killCacheManagers(newServer.getCacheManager());
      TestingUtil.waitForStableTopology(cache(0));

      // Execute one more operation to guarantee topology update on the client
      remote.put(-1, "minus one");
      RoundRobinBalancingStrategy strategy = getLoadBalancer(client(0));
      SocketAddress[] servers = strategy.getServers();

      putHammer.stop = true;
      putHammerFuture.get();

      assertEquals(1, servers.length);
   }

   private class PutHammer implements Callable<Void> {
      volatile boolean stop;

      @Override
      public Void call() throws Exception {
         RemoteCache<Integer, String> remote = client(0).getCache();
         int i = 2;
         while (!stop) {
            remote.put(i, "v" + i);
            i += 1;
         }

         // Rehash completed only signals that server side caches have
         // completed, but the client might not yet have updated its topology.
         // So, run a fair few operations after rehashing has completed server
         // side to verify it's all working fine.
         for (int j = i + 1; j < i + 100 ; j++)
            remote.put(j, "v" + j);

         return null;
      }
   }

}
