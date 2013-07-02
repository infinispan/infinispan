package org.infinispan.tx.totalorder.simple.repl;

import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.repl.SingleNodeSyncPrepareWriteSkewTotalOrderTest")
public class SingleNodeSyncPrepareWriteSkewTotalOrderTest extends SyncPrepareWriteSkewTotalOrderTest {

   public SingleNodeSyncPrepareWriteSkewTotalOrderTest() {
      super(1);
   }
}
