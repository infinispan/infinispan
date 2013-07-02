package org.infinispan.statetransfer;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.ReplOptimisticTxOperationsDuringStateTransferTest")
@CleanupAfterMethod
public class ReplOptimisticTxOperationsDuringStateTransferTest extends BaseOperationsDuringStateTransferTest {

   public ReplOptimisticTxOperationsDuringStateTransferTest() {
      super(CacheMode.REPL_SYNC, true, true);
   }
}
