package org.infinispan.distribution.groups;

import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * It tests the grouping advanced interface in the group's backup owner and in a transactional cache.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "distribution.groups.BackupOwnerReadCommittedTotalOrderGetGroupKeysTest")
public class BackupOwnerReadCommittedTotalOrderGetGroupKeysTest extends BaseTransactionalGetGroupKeysTest {

   public BackupOwnerReadCommittedTotalOrderGetGroupKeysTest() {
      super(TestCacheFactory.BACKUP_OWNER);
   }

   @Override
   protected IsolationLevel getIsolationLevel() {
      return IsolationLevel.READ_COMMITTED;
   }

   @Override
   protected TransactionProtocol getTransactionProtocol() {
      return TransactionProtocol.TOTAL_ORDER;
   }
}
