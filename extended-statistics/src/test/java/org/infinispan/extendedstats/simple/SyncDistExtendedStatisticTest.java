package org.infinispan.extendedstats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.extendedstats.BaseNonTotalOrderClusteredExtendedStatisticsTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "extendedstats.simple.SyncDistExtendedStatisticTest")
public class SyncDistExtendedStatisticTest extends BaseNonTotalOrderClusteredExtendedStatisticsTest {

   public SyncDistExtendedStatisticTest() {
      super(CacheMode.DIST_SYNC);
   }
}
