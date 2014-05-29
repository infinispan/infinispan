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
@Test(groups = "functional", testName = "distribution.groups.PrimaryOwnerRepeatableReadGetGroupKeysTest")
public class PrimaryOwnerRepeatableReadGetGroupKeysTest extends BaseTransactionalGetGroupKeysTest {

   public PrimaryOwnerRepeatableReadGetGroupKeysTest() {
      super(TestCacheFactory.PRIMARY_OWNER);
   }

   @Override
   protected IsolationLevel getIsolationLevel() {
      return IsolationLevel.REPEATABLE_READ;
   }

   @Override
   protected TransactionProtocol getTransactionProtocol() {
      return TransactionProtocol.DEFAULT;
   }

}
