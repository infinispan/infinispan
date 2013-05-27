package org.infinispan.stats.logic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseTxClusterExtendedStatisticLogicTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.logic.Sync2ndPhaseDistTxClusterExtendedStatisticLogicTest")
public class Sync2ndPhaseDistTxClusterExtendedStatisticLogicTest extends BaseTxClusterExtendedStatisticLogicTest {

   public Sync2ndPhaseDistTxClusterExtendedStatisticLogicTest() {
      super(CacheMode.DIST_SYNC, true, false);
   }
}
