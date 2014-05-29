package org.infinispan.distribution.groups;

import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * It tests the grouping advanced interface in the group's non owner and in a transactional cache.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "distribution.groups.NonOwnerRepeatableReadTotalOrderGetGroupKeysTest")
public class NonOwnerRepeatableReadTotalOrderGetGroupKeysTest extends BaseTransactionalGetGroupKeysTest {

   public NonOwnerRepeatableReadTotalOrderGetGroupKeysTest() {
      super(TestCacheFactory.NON_OWNER);
   }

   @Override
   protected IsolationLevel getIsolationLevel() {
      return IsolationLevel.REPEATABLE_READ;
   }

   @Override
   protected TransactionProtocol getTransactionProtocol() {
      return TransactionProtocol.TOTAL_ORDER;
   }

}
