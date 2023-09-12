package org.infinispan.scattered.store;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.DistSyncStoreSharedTest;
import org.infinispan.scattered.Utils;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "unstable", testName = "scattered.store.ScatteredSyncStoreSharedTest")
public class ScatteredSyncStoreSharedTest extends DistSyncStoreSharedTest<ScatteredSyncStoreSharedTest> {
   public ScatteredSyncStoreSharedTest() {
      cacheMode = CacheMode.SCATTERED_SYNC;
      numOwners = 1;
      l1CacheEnabled = false;
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new ScatteredSyncStoreSharedTest().segmented(true),
            new ScatteredSyncStoreSharedTest().segmented(false),
      };
   }

   @Override
   protected void assertOwnershipAndNonOwnership(Object key, boolean allowL1) {
      Utils.assertOwnershipAndNonOwnership(caches, key);
   }

   @Override
   protected int expectedWriteCount() {
      // Scattered cache does not use bulk write and always assumes the write was successful even when store is shared
      // https://issues.redhat.com/browse/ISPN-11963
      return 8;
   }
}
