package org.infinispan.tx.totalorder.simple.repl;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.tx.totalorder.simple.BaseSimpleTotalOrderTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.repl.SyncPrepareUseSynchronizationTotalOrderTest")
public class SyncPrepareUseSynchronizationTotalOrderTest extends BaseSimpleTotalOrderTest {

   public SyncPrepareUseSynchronizationTotalOrderTest() {
      this(3);
   }

   protected SyncPrepareUseSynchronizationTotalOrderTest(int clusterSize) {
      super(clusterSize, CacheMode.REPL_SYNC, false, true);
   }

   @Override
   protected final boolean isOwner(Cache cache, Object key) {
      return true;
   }
}
