package org.infinispan.stats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseClusteredExtendedStatisticTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.Sync2ndPhaseReplExtendedStatisticTest")
public class Sync2ndPhaseReplExtendedStatisticTest extends BaseClusteredExtendedStatisticTest {

   protected Sync2ndPhaseReplExtendedStatisticTest() {
      super(CacheMode.REPL_SYNC, true, false, false);
   }
}
