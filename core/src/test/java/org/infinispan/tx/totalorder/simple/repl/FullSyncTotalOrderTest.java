package org.infinispan.tx.totalorder.simple.repl;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.tx.totalorder.simple.BaseSimpleTotalOrderTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.repl.FullSyncTotalOrderTest")
public class FullSyncTotalOrderTest extends BaseSimpleTotalOrderTest {

   public FullSyncTotalOrderTest() {
      this(3);
   }

   protected FullSyncTotalOrderTest(int clusterSize) {
      super(clusterSize, CacheMode.REPL_SYNC, false, false);
   }

   @Override
   protected final boolean isOwner(Cache cache, Object key) {
      return true;
   }
}
