package org.infinispan.extendedstats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.extendedstats.BaseTotalOrderClusteredExtendedStatisticsTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "extendedstats.simple.TotalOrderSyncReplExtendedStatisticTest")
public class TotalOrderSyncReplExtendedStatisticTest extends BaseTotalOrderClusteredExtendedStatisticsTest {

   public TotalOrderSyncReplExtendedStatisticTest() {
      super(CacheMode.REPL_SYNC);
   }
}
