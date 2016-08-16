package org.infinispan.distribution;

import static org.infinispan.context.Flag.SKIP_REMOTE_LOOKUP;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.replication.AsyncAPINonTxSyncReplTest;
import org.infinispan.test.data.Key;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.AsyncAPINonTxSyncDistTest")
public class AsyncAPINonTxSyncDistTest extends AsyncAPINonTxSyncReplTest {

   @Override
   protected ConfigurationBuilder getConfig() {
      return getDefaultClusteredCacheConfig(sync() ? CacheMode.DIST_SYNC : CacheMode.DIST_ASYNC, false);
   }

   @Override
   protected void assertOnAllCaches(final Key k, final String v, final Cache c1, final Cache c2) {
      if (!sync()) {
         eventually(new Condition() {
            @Override
            public boolean isSatisfied() throws Exception {
               return Util.safeEquals(c1.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).get(k), v) &&
                     Util.safeEquals(c2.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).get(k), v);
            }
         });
      } else {
         Object real;
         assert Util.safeEquals((real = c1.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).get(k)), v) : "Error on cache 1.  Expected " + v + " and got " + real;
         assert Util.safeEquals((real = c2.getAdvancedCache().withFlags(SKIP_REMOTE_LOOKUP).get(k)), v) : "Error on cache 2.  Expected " + v + " and got " + real;
      }
   }
}
