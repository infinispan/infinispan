package org.infinispan.distribution.groups;

import org.infinispan.transaction.TransactionProtocol;
import org.testng.annotations.Test;

/**
 * It tests the grouping advanced interface in the group's backup owner and in a transactional cache.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "distribution.groups.BackupOwnerWriteSkewGetGroupKeysTest")
public class BackupOwnerWriteSkewGetGroupKeysTest extends BaseWriteSkewGetGroupKeysTest {

   public BackupOwnerWriteSkewGetGroupKeysTest() {
      super(TestCacheFactory.BACKUP_OWNER);
   }

   @Override
   protected TransactionProtocol getTransactionProtocol() {
      return TransactionProtocol.DEFAULT;
   }
}
