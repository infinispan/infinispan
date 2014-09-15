package org.infinispan.stress;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertNull;

/**
 * Test the performance of get operations in a replicated cache
 *
 * @author Dan Berindei
 * @since 7.0
 */
@Test(groups = "stress", testName = "stress.ReplGetStressTest")
public class ReplGetStressTest extends MultipleCacheManagersTest {

   private static final int NUM_NODES = 4;
   private static final int NUM_ITERATIONS = Integer.getInteger("operationsCount", 10);
   private static final boolean explicitTxs = Boolean.getBoolean("explicitTxs");

   protected void createCacheManagers() throws Throwable {
      // start the cache managers in the test itself
   }

   public void testStressGetEmptyCache() throws Exception {
      long start = System.nanoTime();
      ConfigurationBuilder replConfig = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      for (int i = 0; i < NUM_NODES; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager();
         defineConfigurationOnAllManagers("repl", replConfig);
         cm.startCaches("repl");
      }
      System.out.println("Caches created: " + manager(0).getMembers());
      System.out.println("Explicit txs: " + explicitTxs);
      System.out.println("Operation count: " + NUM_ITERATIONS + "m");

      // Test the performance of get operations in an empty cache
      Cache<Object, Object> replCache0 = cache(0, "repl");
      for (int i = 0; i < NUM_ITERATIONS * 1000000; i++) {
         if (explicitTxs) tm(0, "repl").begin();
         Object o = replCache0.get("k" + i);
         assertNull(o);
         if (explicitTxs) tm(0, "repl").commit();
      }

      long end = System.nanoTime();
      System.out.println("Test took " + TimeUnit.NANOSECONDS.toSeconds(end - start) + "s");
   }
}
