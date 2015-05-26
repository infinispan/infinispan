package org.infinispan.stats.logic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseTxClusterExtendedStatisticLogicTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.logic.TotalOrderSync2ndPhaseReplTxClusterExtendedStatisticLogicTest")
public class TotalOrderSync2ndPhaseReplTxClusterExtendedStatisticLogicTest extends BaseTxClusterExtendedStatisticLogicTest {

   public TotalOrderSync2ndPhaseReplTxClusterExtendedStatisticLogicTest() {
      super(CacheMode.REPL_SYNC, true, true);
   }
}
