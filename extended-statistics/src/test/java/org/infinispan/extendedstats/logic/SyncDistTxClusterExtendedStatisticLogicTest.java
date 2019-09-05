package org.infinispan.extendedstats.logic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.extendedstats.BaseTxClusterExtendedStatisticLogicTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "extendedstats.logic.SyncDistTxClusterExtendedStatisticLogicTest")
public class SyncDistTxClusterExtendedStatisticLogicTest extends BaseTxClusterExtendedStatisticLogicTest {

   public SyncDistTxClusterExtendedStatisticLogicTest() {
      super(CacheMode.DIST_SYNC, false);
   }
}
