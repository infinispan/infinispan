package org.infinispan.distribution;

import org.infinispan.replication.AsyncAPISyncReplTest;

// For some reason I had to comment this out to ensure the test was disabled!

//@Test(groups = "functional", testName = "distribution.AsyncAPISyncDistTest", enabled = false)
public class AsyncAPISyncDistTest extends AsyncAPISyncReplTest {

//   @SuppressWarnings("unchecked")
//   @Override
//   protected void createCacheManagers() throws Throwable {
//      Configuration c =
//            getDefaultClusteredConfig(sync() ? Configuration.CacheMode.DIST_SYNC : Configuration.CacheMode.DIST_ASYNC);
//      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
//      List<Cache<Key, String>> l = createClusteredCaches(2, getClass().getSimpleName(), c);
//      c1 = l.get(0);
//      c2 = l.get(1);
//   }
//
//   @Override
//   protected void assertOnAllCaches(Key k, String v) {
//      Object real;
//      assert Util.safeEquals((real = c1.getAdvancedCache().get(k, Flag.SKIP_REMOTE_LOOKUP)), v) : "Error on cache 1.  Expected " + v + " and got " + real;
//      assert Util.safeEquals((real = c2.getAdvancedCache().get(k, Flag.SKIP_REMOTE_LOOKUP)), v) : "Error on cache 2.  Expected " + v + " and got " + real;
//   }
}