package org.infinispan.stats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseClusteredExtendedStatisticTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.TotalOrderSyncDistExtendedStatisticTest")
public class TotalOrderSyncDistExtendedStatisticTest extends BaseClusteredExtendedStatisticTest {

   protected TotalOrderSyncDistExtendedStatisticTest() {
      super(CacheMode.DIST_SYNC, false, false, true);
   }
}
