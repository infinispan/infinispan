package org.infinispan.tx;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "tx.OptimisticReadOnlyTxTest")
@CleanupAfterMethod
public class OptimisticReadOnlyTxTest extends ReadOnlyTxTest {

   @Override
   public void testROWhenHasOnlyLocksAndReleasedProperly() throws Exception {
      //no-op. not valid for optimistic transactions
   }

   @Override
   public void testNotROWhenHasWrites() throws Exception {
      //no-op. not valid for optimistic transactions
   }

   @Override
   protected void configure(ConfigurationBuilder builder) {
      super.configure(builder);
      builder.transaction().lockingMode(LockingMode.OPTIMISTIC);
   }

   @Override
   protected int numberCommitCommand() {
      //with optimistic locking, the transactions are committed in 2 phases. So 1 CommitCommand is expected
      return 1;
   }
}
