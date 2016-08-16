package org.infinispan.distribution;

import static org.infinispan.context.Flag.SKIP_REMOTE_LOOKUP;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.replication.AsyncAPITxSyncReplTest;
import org.infinispan.test.data.Key;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.AsyncAPITxSyncDistTest")
public class AsyncAPITxSyncDistTest extends AsyncAPITxSyncReplTest {

   @Override
   protected ConfigurationBuilder getConfig() {
      return getDefaultClusteredCacheConfig(sync() ? CacheMode.DIST_SYNC : CacheMode.DIST_ASYNC, true);
   }

   @Override
   protected void assertOnAllCaches(Key k, String v, Cache c1, Cache c2) {
      Object real;
      assert Util.safeEquals((real = c1.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).get(k)), v) : "Error on cache 1.  Expected " + v + " and got " + real;
      assert Util.safeEquals((real = c2.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).get(k)), v) : "Error on cache 2.  Expected " + v + " and got " + real;
   }
}