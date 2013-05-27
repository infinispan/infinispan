package org.infinispan.stats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseClusteredExtendedStatisticTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.TotalOrderSync2ndPhaseReplExtendedStatisticTest")
public class TotalOrderSync2ndPhaseReplExtendedStatisticTest extends BaseClusteredExtendedStatisticTest {

   protected TotalOrderSync2ndPhaseReplExtendedStatisticTest() {
      super(CacheMode.REPL_SYNC, true, false, true);
   }
}
