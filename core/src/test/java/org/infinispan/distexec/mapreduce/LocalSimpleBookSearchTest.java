package org.infinispan.distexec.mapreduce;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests the mapreduce with simple configuration with a local cache with a transport.
 *
 * @author William Burns
 * @since 5.3
 */
@Test(groups = "functional", testName = "distexec.mapreduce.LocalSimpleBookSearchTest")
public class LocalSimpleBookSearchTest extends BookSearchTest {

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.LOCAL, true);
      createClusteredCaches(1, "bookSearch" ,builder);
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
