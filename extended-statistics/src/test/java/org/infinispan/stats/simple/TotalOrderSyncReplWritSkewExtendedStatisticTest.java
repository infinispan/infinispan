package org.infinispan.stats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseTotalOrderClusteredExtendedStatisticsTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.TotalOrderSyncReplWritSkewExtendedStatisticTest")
public class TotalOrderSyncReplWritSkewExtendedStatisticTest extends BaseTotalOrderClusteredExtendedStatisticsTest {

   public TotalOrderSyncReplWritSkewExtendedStatisticTest() {
      super(CacheMode.REPL_SYNC, true);
   }
}
