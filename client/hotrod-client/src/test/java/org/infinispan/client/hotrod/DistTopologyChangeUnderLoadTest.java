package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.net.SocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "client.hotrod.DistKeepRunningWithTopologyChangeTest")
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

   public void testPutsSucceedWhileTopologyChanges() throws Exception {
      RemoteCache<Integer, String> remote = client(0).getCache();
      remote.put(1, "v1");
      assertEquals("v1", remote.get(1));

      PutHammer putHammer = new PutHammer();
      Future<Void> putHammerFuture = fork(putHammer);

      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(getCacheConfiguration());
      registerCacheManager(cm);
      TestHelper.startHotRodServer(cm);
      waitForClusterToForm();
      TestingUtil.waitForRehashToComplete(cm.getCache(), cache(0));

      // Wait for 2 seconds
      TestingUtil.sleepThread(2000);

      TestingUtil.killCacheManagers(cm);
      TestingUtil.waitForRehashToComplete(cache(0));

      TcpTransportFactory transportFactory = TestingUtil.extractField(client(0), "transportFactory");
      SocketAddress[] servers = ((RoundRobinBalancingStrategy) transportFactory.getBalancer(RemoteCacheManager.cacheNameBytes())).getServers();

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
