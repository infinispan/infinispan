package org.infinispan.api.mvcc;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.mvcc.PutForExternalReadDistOptimisticTest")
@CleanupAfterMethod
public class PutForExternalReadDistOptimisticTest extends PutForExternalReadTxBaseTest {

   @Override
   protected ConfigurationBuilder createCacheConfigBuilder() {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      c.clustering().hash().numOwners(100).numSegments(4).l1().disable();
      return c;
   }
}
