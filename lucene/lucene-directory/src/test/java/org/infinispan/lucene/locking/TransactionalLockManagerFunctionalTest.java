package org.infinispan.lucene.locking;

import org.apache.lucene.store.LockFactory;
import org.infinispan.Cache;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * TransactionalLockManagerFunctionalTest.
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
@SuppressWarnings("unchecked")
@Test(groups = "functional", testName = "lucene.locking.TransactionalLockManagerFunctionalTest")
public class TransactionalLockManagerFunctionalTest extends LockManagerFunctionalTest {

   @Override
   protected LockFactory makeLockFactory(Cache cache, String commonIndexName) {
      return new TransactionalLockFactory(cache, commonIndexName);
   }

   @Override
   protected TransactionMode getTransactionsMode() {
      return TransactionMode.TRANSACTIONAL;
   }

}
