package org.infinispan.client.hotrod.retry;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.testng.annotations.Test;

/**
 * @author Dan Berindei
 * @since 8.1
 */
@Test (testName = "client.hotrod.retry.ReplicationHitsTest", groups = "functional")
public class ReplicationHitsTest extends AbstractRetryTest {

   public static final int NUM_WRITES = 100;

   public void testPut() {
      resetStats();
      assertNoHits();

      for (Cache c : caches()) {
         ((RpcManagerImpl) c.getAdvancedCache().getRpcManager()).setStatisticsEnabled(true);
      }

      for (int i = 0; i < NUM_WRITES; i++) {
         remoteCache.put("k" + i, "v1");
      }

      int totalReplications = 0;
      for (Cache c : caches()) {
         totalReplications += ((RpcManagerImpl) c.getAdvancedCache().getRpcManager()).getReplicationCount();
      }
      assertEquals(NUM_WRITES, totalReplications);
   }

   @Override
   protected ConfigurationBuilder getCacheConfig() {
      ConfigurationBuilder config =
            getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      config.clustering().hash().numSegments(60);
      return config;
   }
}
