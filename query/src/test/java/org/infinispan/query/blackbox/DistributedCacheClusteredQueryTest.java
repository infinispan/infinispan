package org.infinispan.query.blackbox;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Testing Clustered distributed queries on Distributed  cache.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.DistributedCacheClusteredQueryTest")
public class DistributedCacheClusteredQueryTest extends ClusteredQueryTest {

   public CacheMode getCacheMode() {
      return CacheMode.DIST_SYNC;
   }
}
