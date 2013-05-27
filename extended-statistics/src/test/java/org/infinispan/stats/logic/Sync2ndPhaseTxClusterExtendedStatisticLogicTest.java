package org.infinispan.stats.logic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseTxClusterExtendedStatisticLogicTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.logic.Sync2ndPhaseReplTxClusterExtendedStatisticLogicTest")
public class Sync2ndPhaseTxClusterExtendedStatisticLogicTest extends BaseTxClusterExtendedStatisticLogicTest {

   public Sync2ndPhaseTxClusterExtendedStatisticLogicTest() {
      super(CacheMode.REPL_SYNC, true, false);
   }
}
