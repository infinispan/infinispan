package org.infinispan.tx.synchronisation;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.XaTransactionTable;
import org.infinispan.tx.TransactionsSpanningCaches;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test(groups = "functional", testName = "tx.synchronisation.TransactionsSpanningCachesSyncTest")
public class TransactionsSpanningCachesSyncTest extends TransactionsSpanningCaches {

   @Override
   protected void amendConfig(ConfigurationBuilder defaultCacheConfig) {
      defaultCacheConfig.transaction().useSynchronization(true);
   }

   public void testSyncIsUsed() {
      assert cache.getCacheConfiguration().transaction().useSynchronization();;
      TransactionTable transactionTable = TestingUtil.extractComponent(cache, TransactionTable.class);
      assert !(transactionTable instanceof XaTransactionTable);
   }
}
