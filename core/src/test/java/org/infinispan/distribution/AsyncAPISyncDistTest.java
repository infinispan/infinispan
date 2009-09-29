package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.replication.AsyncAPISyncReplTest;
import org.infinispan.test.data.Key;
import org.infinispan.util.Util;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Test(groups = "functional", testName = "distribution.AsyncAPISyncDistTest")
public class AsyncAPISyncDistTest extends AsyncAPISyncReplTest {

   @SuppressWarnings("unchecked")
   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration c =
            getDefaultClusteredConfig(sync() ? Configuration.CacheMode.DIST_SYNC : Configuration.CacheMode.DIST_ASYNC, true);
      c.setLockAcquisitionTimeout(30, TimeUnit.SECONDS);
      List<Cache<Key, String>> l = createClusteredCaches(2, getClass().getSimpleName(), c);
      c1 = l.get(0);
      c2 = l.get(1);
   }

   @Override
   protected void assertOnAllCaches(Key k, String v) {
      Object real;
      assert Util.safeEquals((real = c1.getAdvancedCache().get(k, Flag.SKIP_REMOTE_LOOKUP)), v) : "Error on cache 1.  Expected " + v + " and got " + real;
      assert Util.safeEquals((real = c2.getAdvancedCache().get(k, Flag.SKIP_REMOTE_LOOKUP)), v) : "Error on cache 2.  Expected " + v + " and got " + real;
   }
}