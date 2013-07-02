package org.infinispan.tx.totalorder.simple.repl;

import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.repl.SingleNodeFullSyncWriteSkewTotalOrderTest")
public class SingleNodeFullSyncWriteSkewTotalOrderTest extends FullSyncWriteSkewTotalOrderTest {

   public SingleNodeFullSyncWriteSkewTotalOrderTest() {
      super(1);
   }
}
