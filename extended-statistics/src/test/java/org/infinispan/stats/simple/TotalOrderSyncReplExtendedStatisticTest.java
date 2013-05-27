package org.infinispan.stats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseClusteredExtendedStatisticTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.TotalOrderSyncReplExtendedStatisticTest")
public class TotalOrderSyncReplExtendedStatisticTest extends BaseClusteredExtendedStatisticTest {

   protected TotalOrderSyncReplExtendedStatisticTest() {
      super(CacheMode.REPL_SYNC, false, false, true);
   }
}
