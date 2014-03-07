package org.infinispan.distexec.mapreduce;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
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

   public void testInvokeMapReduceOnSubsetOfKeysWithResultCache() throws Exception {
      String cacheName = "resultCache2";
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.LOCAL, true);
      defineConfigurationOnAllManagers(cacheName, builder);
      try {
         MapReduceTask<String, String, String, Integer> task = invokeMapReduce(new String[] { "1", "2", "3" });
         task.execute(cacheName);
         Cache c1 = cache(0, cacheName);
         assertPartialWordCount(countWords(c1));
         c1.clear();
      } finally {
         removeCacheFromCluster(cacheName);
      }
   }

   public void testInvokeMapReduceOnAllKeysWithResultCache() throws Exception {
      String cacheName = "resultCache";
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.LOCAL, true);
      defineConfigurationOnAllManagers(cacheName, builder);
      try {
         MapReduceTask<String, String, String, Integer> task = invokeMapReduce(null);
         Cache c1 = cache(0, cacheName);
         task.execute(c1);
         verifyResults(c1);
         c1.clear();
      } finally {
         removeCacheFromCluster(cacheName);
      }
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
