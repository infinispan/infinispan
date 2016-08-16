package org.infinispan.tx.totalorder.simple.repl;

import static org.testng.Assert.assertFalse;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.tx.totalorder.simple.BaseSimpleTotalOrderTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.repl.FullSyncWriteSkewTotalOrderTest")
public class FullSyncWriteSkewTotalOrderTest extends BaseSimpleTotalOrderTest {

   public FullSyncWriteSkewTotalOrderTest() {
      this(3);
   }

   protected FullSyncWriteSkewTotalOrderTest(int clusterSize) {
      super(clusterSize, CacheMode.REPL_SYNC, true, true, false);
   }

   @Override
   public final void testSinglePhaseTotalOrder() {
      assertFalse(Configurations.isOnePhaseTotalOrderCommit(cache(0).getCacheConfiguration()));
   }

   @Override
   protected final boolean isOwner(Cache cache, Object key) {
      return true;
   }
}
