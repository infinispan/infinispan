package org.infinispan.stats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseNonTotalOrderClusteredExtendedStatisticsTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.SyncReplExtendedStatisticTest", enabled = false, description = "To be fixed by ISPN-6468")
public class SyncReplExtendedStatisticTest extends BaseNonTotalOrderClusteredExtendedStatisticsTest {

   public SyncReplExtendedStatisticTest() {
      super(CacheMode.REPL_SYNC, false, false);
   }
}
