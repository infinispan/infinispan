package org.infinispan.tx.totalorder.simple.repl;

import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.repl.SingleNodeFullSyncTotalOrderTest")
public class SingleNodeFullSyncTotalOrderTest extends FullSyncTotalOrderTest {

   public SingleNodeFullSyncTotalOrderTest() {
      super(1);
   }
}
