package org.infinispan.tx.totalorder.simple.repl;

import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.repl.SingleNodeSyncPrepareUseSynchronizationTotalOrderTest")
public class SingleNodeSyncPrepareUseSynchronizationTotalOrderTest extends SyncPrepareUseSynchronizationTotalOrderTest {

   public SingleNodeSyncPrepareUseSynchronizationTotalOrderTest() {
      super(1);
   }
}
