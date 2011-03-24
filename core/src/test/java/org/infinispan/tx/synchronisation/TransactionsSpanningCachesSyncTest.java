package org.infinispan.tx.synchronisation;

import org.infinispan.config.Configuration;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.XaTransactionTable;
import org.infinispan.tx.TransactionsSpanningCaches;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test(groups = "functional", testName = "tx.synchronization.TransactionsSpanningCachesSyncTest")
public class TransactionsSpanningCachesSyncTest extends TransactionsSpanningCaches {

   @Override
   protected void amendConfig(Configuration defaultCacheConfig) {
      defaultCacheConfig.configureTransaction().useSynchronization(true);
   }

   public void testSyncIsUsed() {
      assert cache.getConfiguration().isUseSynchronizationForTransactions();
      TransactionTable transactionTable = TestingUtil.extractComponent(cache, TransactionTable.class);
      assert !(transactionTable instanceof XaTransactionTable);
   }
}
