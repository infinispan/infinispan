package org.infinispan.stats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseClusteredExtendedStatisticTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.AsyncDistExtendedStatisticTest")
public class AsyncDistExtendedStatisticTest extends BaseClusteredExtendedStatisticTest {

   protected AsyncDistExtendedStatisticTest() {
      super(CacheMode.DIST_ASYNC, false, false, false);
   }

   @Test(groups = "unstable")
   @Override
   public void testReplaceWithOldVal() {
      super.testReplaceWithOldVal();
   }
}
