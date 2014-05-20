package org.infinispan.stress;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Test that we're able to start a large cluster in a single JVM.
 *
 * @author Dan Berindei
 * @since 5.3
 */
@Test(groups = "stress", testName = "stress.LargeClusterStressTest")
public class LargeClusterStressTest extends MultipleCacheManagersTest {

   private static final int NUM_NODES = 64;

   @Override
   protected void createCacheManagers() throws Throwable {
      // start the cache managers in the test itself
   }

   public void testLargeCluster() {
      ConfigurationBuilder distConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      ConfigurationBuilder replConfig = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      for (int i = 0; i < NUM_NODES; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager();
         defineConfigurationOnAllManagers("dist", distConfig);
         defineConfigurationOnAllManagers("repl", replConfig);
         Cache<Object,Object> replCache = cm.getCache("repl");
         Cache<Object, Object> distCache = cm.getCache("dist");

         replCache.put(cm.getAddress(), "bla");

         waitForClusterToForm("repl");
         waitForClusterToForm("dist");
      }
   }
}
