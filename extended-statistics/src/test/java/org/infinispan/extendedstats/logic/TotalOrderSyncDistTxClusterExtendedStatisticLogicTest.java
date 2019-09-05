package org.infinispan.extendedstats.logic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.extendedstats.BaseTxClusterExtendedStatisticLogicTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "extendedstats.logic.TotalOrderSyncDistTxClusterExtendedStatisticLogicTest")
public class TotalOrderSyncDistTxClusterExtendedStatisticLogicTest extends BaseTxClusterExtendedStatisticLogicTest {

   public TotalOrderSyncDistTxClusterExtendedStatisticLogicTest() {
      super(CacheMode.DIST_SYNC, true);
   }
}
