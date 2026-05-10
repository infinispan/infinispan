package org.infinispan.tx;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.batch.BatchContainer;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.tm.BatchModeTransactionManager;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.testng.annotations.Test;

import jakarta.transaction.TransactionManager;

/**
 * @author Mircea Markus &lt;mircea.markus@jboss.com&gt; (C) 2011 Red Hat Inc.
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
      assertInstanceOf(BatchModeTransactionManager.class, tm);
      TransactionTable tt = TestingUtil.getTransactionTable(cache);
      assertSame(TransactionTable.class, tt.getClass());
      BatchContainer bc = TestingUtil.extractComponent(cache, BatchContainer.class);

      cache.startBatch();
      cache.put("k", "v");
      assertTrue(getBatchTx(bc).getEnlistedSynchronization().size() == 1);
      assertTrue(getBatchTx(bc).getEnlistedResources().isEmpty());
      cache.endBatch(true);
      assertNull(getBatchTx(bc));
   }

   private EmbeddedTransaction getBatchTx(BatchContainer bc) {
      return (EmbeddedTransaction) bc.getBatchTransaction();
   }
}
