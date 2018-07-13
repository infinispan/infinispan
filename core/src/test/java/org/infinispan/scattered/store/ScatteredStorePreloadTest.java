package org.infinispan.scattered.store;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.DistStorePreloadTest;
import org.infinispan.scattered.Utils;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional", testName = "scattered.store.ScatteredStorePreloadTest")
public class ScatteredStorePreloadTest extends DistStorePreloadTest<ScatteredStorePreloadTest> {

   public ScatteredStorePreloadTest() {
      cacheMode = CacheMode.SCATTERED_SYNC;
      numOwners = 1;
      l1CacheEnabled = false;
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new ScatteredStorePreloadTest().segmented(true),
            new ScatteredStorePreloadTest().segmented(false),
      };
   }

   @Override
   protected void assertOwnershipAndNonOwnership(Object key, boolean allowL1) {
      Utils.assertOwnershipAndNonOwnership(caches, key);
   }

}
