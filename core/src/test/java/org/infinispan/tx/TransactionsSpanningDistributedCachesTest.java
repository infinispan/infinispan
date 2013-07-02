package org.infinispan.tx;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.0
 */
@Test(groups = "functional", testName = "tx.TransactionsSpanningDistributedCachesTest")
public class TransactionsSpanningDistributedCachesTest extends TransactionsSpanningReplicatedCachesTest {

   @Override
   protected ConfigurationBuilder getConfiguration() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   @Override
   protected void createCacheManagers() throws Exception {
      super.createCacheManagers();
      cache(0, "cache1");
      cache(0, "cache2");
      cache(1, "cache1");
      cache(1, "cache2");
      cache(0);
      cache(1);
   }

}
