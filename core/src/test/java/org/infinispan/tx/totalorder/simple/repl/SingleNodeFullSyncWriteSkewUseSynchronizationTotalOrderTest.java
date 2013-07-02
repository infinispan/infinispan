package org.infinispan.tx.totalorder.simple.repl;

import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.repl.SingleNodeFullSyncWriteSkewUseSynchronizationTotalOrderTest")
public class SingleNodeFullSyncWriteSkewUseSynchronizationTotalOrderTest extends FullSyncWriteSkewUseSynchronizationTotalOrderTest {

   public SingleNodeFullSyncWriteSkewUseSynchronizationTotalOrderTest() {
      super(1);
   }
}
