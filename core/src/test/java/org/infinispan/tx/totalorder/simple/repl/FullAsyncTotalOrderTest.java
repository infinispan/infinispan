package org.infinispan.tx.totalorder.simple.repl;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.tx.totalorder.simple.BaseSimpleTotalOrderTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.repl.FullAsyncTotalOrderTest")
public class FullAsyncTotalOrderTest extends BaseSimpleTotalOrderTest {

   public FullAsyncTotalOrderTest() {
      this(3);
   }

   protected FullAsyncTotalOrderTest(int clusterSize) {
      super(clusterSize, CacheMode.REPL_ASYNC, false, false, false);
   }

   @Override
   protected final boolean isOwner(Cache cache, Object key) {
      return true;
   }
}
