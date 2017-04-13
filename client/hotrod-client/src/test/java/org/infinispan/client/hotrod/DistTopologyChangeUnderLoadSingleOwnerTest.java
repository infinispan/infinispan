package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.DistTopologyChangeUnderLoadSingleOwnerTest")
public class DistTopologyChangeUnderLoadSingleOwnerTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(2, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(1);
      return hotRodCacheConfiguration(builder);
   }

   @Override
   protected int maxRetries() {
      return 1;
   }

   public void testRestartServerWhilePutting() throws Exception {
      RemoteCache<Integer, String> remote = client(0).getCache();
      remote.put(1, "v1");
      assertEquals("v1", remote.get(1));

      PutHammer putHammer = new PutHammer();
      Future<Void> putHammerFuture = fork(putHammer);

      // Kill server
      HotRodServer toKillServer = servers.get(0);
      HotRodClientTestingUtil.killServers(toKillServer);
      servers.remove(toKillServer);
      EmbeddedCacheManager toKillCacheManager = cacheManagers.get(0);
      TestingUtil.killCacheManagers(toKillCacheManager);
      cacheManagers.remove(toKillCacheManager);
      TestingUtil.waitForStableTopology(cache(0));

      // Start server
      addHotRodServer(getCacheConfiguration());

      putHammer.stop = true;
      putHammerFuture.get();
   }

   private class PutHammer implements Callable<Void> {
      private final Random r = new Random();
      volatile boolean stop;

      @Override
      public Void call() throws Exception {
         RemoteCache<Integer, String> remote = client(0).getCache();
         while (!stop) {
            int i = r.nextInt(10);
            remote.put(i, "v" + i);
         }

         return null;
      }
   }

}
