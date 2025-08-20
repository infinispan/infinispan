package org.infinispan.query.timeout;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.TimeoutException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.timeout.LocalNonIndexedTimeoutTest")
public class LocalNonIndexedTimeoutTest extends LocalIndexedTimeoutTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, getDefaultStandaloneCacheConfig(false));
   }

   @Test(expectedExceptions = TimeoutException.class)
   public void testTimeout() {
      TestHelper.runRegularQueryWithTimeout(cache, 1, TimeUnit.NANOSECONDS);
   }

   @Test(expectedExceptions = TimeoutException.class)
   public void testTimeoutSortedQuery() {
      TestHelper.runRegularSortedQueryWithTimeout(cache, 1, TimeUnit.NANOSECONDS);
   }
}
