package org.infinispan.tx.synchronisation;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.XaTransactionTable;
import org.infinispan.tx.TransactionsSpanningCachesTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test(groups = "functional", testName = "tx.synchronisation.TransactionsSpanningCachesSyncTest")
public class TransactionsSpanningCachesSyncTest extends TransactionsSpanningCachesTest {
   public Object[] factory() {
      return new Object[] {
            new TransactionsSpanningCachesSyncTest().withStorage(StorageType.HEAP, StorageType.HEAP),
            new TransactionsSpanningCachesSyncTest().withStorage(StorageType.OFF_HEAP, StorageType.OFF_HEAP),
            new TransactionsSpanningCachesSyncTest().withStorage(StorageType.HEAP, StorageType.OFF_HEAP)
      };
   }

   @Override
   protected void amendConfig(ConfigurationBuilder defaultCacheConfig) {
      defaultCacheConfig.transaction().useSynchronization(true);
   }

   public void testSyncIsUsed() {
      assert cacheManagers.get(0).getCache().getCacheConfiguration().transaction().useSynchronization();
      TransactionTable transactionTable = TestingUtil.extractComponent(cacheManagers.get(0).getCache(), TransactionTable.class);
      assert !(transactionTable instanceof XaTransactionTable);
   }
}
