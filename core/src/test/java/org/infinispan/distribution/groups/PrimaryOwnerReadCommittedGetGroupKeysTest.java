package org.infinispan.distribution.groups;

import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * It tests the grouping advanced interface in the group's primary owner and in a transactional cache.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "distribution.groups.PrimaryOwnerReadCommittedGetGroupKeysTest")
public class PrimaryOwnerReadCommittedGetGroupKeysTest extends BaseTransactionalGetGroupKeysTest {

   public PrimaryOwnerReadCommittedGetGroupKeysTest() {
      super(TestCacheFactory.PRIMARY_OWNER);
   }

   @Override
   protected IsolationLevel getIsolationLevel() {
      return IsolationLevel.READ_COMMITTED;
   }

   @Override
   protected TransactionProtocol getTransactionProtocol() {
      return TransactionProtocol.DEFAULT;
   }

}
