package org.infinispan.tx;

import static org.testng.Assert.assertEquals;

import javax.transaction.TransactionManager;

import org.infinispan.batch.BatchContainer;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.tm.BatchModeTransactionManager;
import org.infinispan.transaction.tm.DummyTransaction;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.1
 */
@Test (groups = "functional", testName = "tx.BatchingAndEnlistmentTest")
public class BatchingAndEnlistmentTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.invocationBatching().enable();
      cb.transaction().transactionManagerLookup(null);
      return TestCacheManagerFactory.createCacheManager(cb);
   }

   public void testExpectedEnlistmentMode() {
      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      assert tm instanceof BatchModeTransactionManager;
      TransactionTable tt = TestingUtil.getTransactionTable(cache);
      assertEquals(tt.getClass(), TransactionTable.class);
      BatchContainer bc = TestingUtil.extractComponent(cache, BatchContainer.class);

      cache.startBatch();
      cache.put("k", "v");
      assert getBatchTx(bc).getEnlistedSynchronization().size() == 1;
      assert getBatchTx(bc).getEnlistedResources().size() == 0;
      cache.endBatch(true);
      assert getBatchTx(bc) == null;
   }

   private DummyTransaction getBatchTx(BatchContainer bc) {
      return (DummyTransaction) bc.getBatchTransaction();
   }
}
