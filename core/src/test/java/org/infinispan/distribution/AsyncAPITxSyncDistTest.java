package org.infinispan.distribution;

import static org.infinispan.context.Flag.SKIP_REMOTE_LOOKUP;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.replication.AsyncAPITxSyncReplTest;
import org.infinispan.test.data.Key;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.AsyncAPITxSyncDistTest")
public class AsyncAPITxSyncDistTest extends AsyncAPITxSyncReplTest {

   @Override
   protected ConfigurationBuilder getConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   @Override
   protected void assertOnAllCaches(Key k, String v, Cache c1, Cache c2) {
      assertEquals("Error in cache 1.", v, c1.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).get(k));
      assertEquals("Error in cache 2,", v, c2.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).get(k));
   }
}
