package org.infinispan.api;

import org.infinispan.Cache;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Test that verifies the behaviour of Cache and CacheContainer.getCache() calls after
 * Cache and CacheContainer instances have been stopped. This emulates redeployment
 * scenarios under a situations where the CacheContainer is a shared resource.
 *
 * @author Galder Zamarre�o
 * @since 4.2
 */
@Test(groups = "functional", testName = "api.TerminatedCacheTest")
public class TerminatedCacheTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(false);
   }

   @Test(expectedExceptions = IllegalLifecycleStateException.class)
   public void testCacheStopFollowedByGetCache() {
      Cache<String, String> cache = cacheManager.getCache();
      cache.put("k", "v");
      cache.stop();
      Cache<String, String> cache2 = cacheManager.getCache();
      cache2.put("k", "v2");
   }

   @Test(expectedExceptions = IllegalLifecycleStateException.class)
   public void testCacheStopFollowedByCacheOp() {
      cacheManager.defineConfiguration("big", cacheManager.getDefaultCacheConfiguration());
      Cache<String, String> cache = cacheManager.getCache("big");
      cache.put("k", "v");
      cache.stop();
      cache.put("k", "v2");
   }
}
