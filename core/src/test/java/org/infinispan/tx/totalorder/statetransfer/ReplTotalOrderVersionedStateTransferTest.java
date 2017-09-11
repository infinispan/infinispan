package org.infinispan.tx.totalorder.statetransfer;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.versioning.VersionedReplStateTransferTest;
import org.infinispan.transaction.TransactionProtocol;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.3
 */
@Test(groups = "unstable", testName = "tx.totalorder.statetransfer.ReplTotalOrderVersionedStateTransferTest", description = "ISPN-6827,functional group")
public class ReplTotalOrderVersionedStateTransferTest extends VersionedReplStateTransferTest {

   @Override
   protected void amendConfig(ConfigurationBuilder dcc) {
      dcc.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER).recovery().disable();
   }

   @Test(groups = "unstable")
   @Override
   public void testStateTransfer() throws Exception {
      super.testStateTransfer();
   }
}
