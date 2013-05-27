package org.infinispan.stats.logic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseTxClusterExtendedStatisticLogicTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.logic.TotalOrderAsyncReplTxClusterExtendedStatisticLogicTest")
public class TotalOrderAsyncTxClusterExtendedStatisticLogicTest extends BaseTxClusterExtendedStatisticLogicTest {

   public TotalOrderAsyncTxClusterExtendedStatisticLogicTest() {
      super(CacheMode.REPL_ASYNC, false, true);
   }
}
