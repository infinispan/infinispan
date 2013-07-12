package org.infinispan.distexec.mapreduce;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * ReplicatedFourNodesMapReduceTest tests Map/Reduce functionality using four Infinispan nodes,
 * replicated reduce and individual per task intermediate key/value cache
 *
 * @author William Burns
 * @since 5.3
 */
@Test(groups = "functional", testName = "distexec.mapreduce.LocalMapReduceTest")
public class LocalMapReduceTest extends DistributedFourNodesMapReduceTest {

   @Override
   protected CacheMode getCacheMode() {
      return CacheMode.LOCAL;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(false);
      cacheManagers.add(cacheManager);
   }

   @Override
   /**
    * Method is overridden so that there is 1 cache for what the test may think are different managers
    * since local only has 1 manager.
    */
   protected <A, B> Cache<A, B> cache(int index, String cacheName) {
      return super.cache(0, cacheName);
   }
}
