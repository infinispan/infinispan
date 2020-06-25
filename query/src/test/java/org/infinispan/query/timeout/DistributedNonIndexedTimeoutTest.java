package org.infinispan.query.timeout;

import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.SearchTimeoutException;
import org.infinispan.query.test.QueryTestSCI;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.timeout.DistributedNonIndexedTimeoutTest")
public class DistributedNonIndexedTimeoutTest extends DistributedIndexedTimeoutTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      createClusteredCaches(2, QueryTestSCI.INSTANCE, cacheCfg);
      cache1 = cache(0);
   }

   @Test(expectedExceptions = SearchTimeoutException.class)
   public void testTimeout() {
      TestHelper.runRegularQueryWithTimeout(cache1, 1, TimeUnit.NANOSECONDS);
   }

   @Test(expectedExceptions = SearchTimeoutException.class)
   public void testTimeoutSortedQuery() {
      TestHelper.runRegularSortedQueryWithTimeout(cache1, 1, TimeUnit.NANOSECONDS);
   }
}
