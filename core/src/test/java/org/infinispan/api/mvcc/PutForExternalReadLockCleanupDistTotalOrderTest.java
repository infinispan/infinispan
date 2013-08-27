package org.infinispan.api.mvcc;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionProtocol;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.mvcc.PutForExternalReadLockCleanupDistTotalOrderTest")
@CleanupAfterMethod
public class PutForExternalReadLockCleanupDistTotalOrderTest extends PutForExternalReadLockCleanupTest {

   @Override
   protected final boolean transactional() {
      return true;
   }

   @Override
   protected void amendConfiguration(ConfigurationBuilder builder) {
      builder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER)
            .recovery().disable();
   }
}
