package org.infinispan.statetransfer;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.ReplPessimisticOperationsDuringStateTransferTest")
@CleanupAfterMethod
public class ReplPessimisticOperationsDuringStateTransferTest extends BaseOperationsDuringStateTransferTest {

   public ReplPessimisticOperationsDuringStateTransferTest() {
      super(CacheMode.REPL_SYNC, true, false);
   }
}
