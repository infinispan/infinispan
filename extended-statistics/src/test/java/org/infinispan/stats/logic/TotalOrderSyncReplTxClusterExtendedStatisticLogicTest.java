package org.infinispan.stats.logic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseTxClusterExtendedStatisticLogicTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.logic.TotalOrderSyncReplTxClusterExtendedStatisticLogicTest")
public class TotalOrderSyncReplTxClusterExtendedStatisticLogicTest extends BaseTxClusterExtendedStatisticLogicTest {

   public TotalOrderSyncReplTxClusterExtendedStatisticLogicTest() {
      super(CacheMode.REPL_SYNC, false, true);
   }
}
