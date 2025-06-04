package org.infinispan.tx.synchronisation;

import static org.testng.Assert.assertEquals;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.tx.LocalModeTxTest;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import jakarta.transaction.NotSupportedException;
import jakarta.transaction.SystemException;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test(groups = "functional", testName = "tx.synchronisation.LocalModeWithSyncTxTest")
public class LocalModeWithSyncTxTest extends LocalModeTxTest {

   @Factory
   public Object[] factory() {
      return new Object[] {
            new LocalModeWithSyncTxTest().withStorage(StorageType.BINARY),
            new LocalModeWithSyncTxTest().withStorage(StorageType.HEAP),
            new LocalModeWithSyncTxTest().withStorage(StorageType.OFF_HEAP)
      };
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder config = getDefaultStandaloneCacheConfig(true);
      config.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup()).useSynchronization(true);
      return TestCacheManagerFactory.createCacheManager(config);
   }

   public void testSyncRegisteredWithCommit() throws Exception {
      EmbeddedTransaction dt = startTx();
      tm().commit();
      assertEquals(0, dt.getEnlistedResources().size());
      assertEquals(0, dt.getEnlistedSynchronization().size());
      assertEquals("v", cache.get("k"));
   }

   public void testSyncRegisteredWithRollback() throws Exception {
      EmbeddedTransaction dt = startTx();
      tm().rollback();
      assertEquals(null, cache.get("k"));
      assertEquals(0, dt.getEnlistedResources().size());
      assertEquals(0, dt.getEnlistedSynchronization().size());
   }

   private EmbeddedTransaction startTx() throws NotSupportedException, SystemException {
      tm().begin();
      cache.put("k","v");
      EmbeddedTransaction dt = (EmbeddedTransaction) tm().getTransaction();
      assertEquals(0, dt.getEnlistedResources().size());
      assertEquals(1, dt.getEnlistedSynchronization().size());
      cache.put("k2","v2");
      assertEquals(0, dt.getEnlistedResources().size());
      assertEquals(1, dt.getEnlistedSynchronization().size());
      return dt;
   }
}
