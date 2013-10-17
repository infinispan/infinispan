package org.infinispan.api.mvcc;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * PutForExternalRead tests for invalidated caches.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Test(groups="functional", testName = "api.mvcc.PutForExternalReadInvTest")
public class PutForExternalReadInvTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
            .clustering().cacheMode(CacheMode.INVALIDATION_SYNC)
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      createClusteredCaches(2, builder);
   }

   public void testReadOwnWrites() {
      Cache<Integer, String> c0 = cache(0);
      Cache<Integer, String> c1 = cache(1);
      c0.putForExternalRead(1, "v1");
      assertEquals("v1", c0.get(1));
      c1.putForExternalRead(1, "v1");
      assertEquals("v1", c1.get(1));
   }

}
