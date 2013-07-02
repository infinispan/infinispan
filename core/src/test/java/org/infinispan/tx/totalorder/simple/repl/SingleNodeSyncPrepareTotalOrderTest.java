package org.infinispan.tx.totalorder.simple.repl;

import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.repl.SingleNodeSyncPrepareTotalOrderTest")
public class SingleNodeSyncPrepareTotalOrderTest extends SyncPrepareTotalOrderTest {

   public SingleNodeSyncPrepareTotalOrderTest() {
      super(1);
   }
}
