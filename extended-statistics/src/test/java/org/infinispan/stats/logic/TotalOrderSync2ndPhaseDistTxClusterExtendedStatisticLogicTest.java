package org.infinispan.stats.logic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseTxClusterExtendedStatisticLogicTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.logic.TotalOrderSync2ndPhaseDistTxClusterExtendedStatisticLogicTest")
public class TotalOrderSync2ndPhaseDistTxClusterExtendedStatisticLogicTest extends BaseTxClusterExtendedStatisticLogicTest {

   public TotalOrderSync2ndPhaseDistTxClusterExtendedStatisticLogicTest() {
      super(CacheMode.DIST_SYNC, true, true);
   }
}
