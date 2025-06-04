package org.infinispan.tx;

import static org.infinispan.test.TestingUtil.getTransactionTable;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.impl.TransactionTable;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

@Test(groups = "functional", testName = "tx.LocalModeTxTest")
public class LocalModeTxTest extends SingleCacheManagerTest {

   protected StorageType storage;

   @Factory
   public Object[] factory() {
      return new Object[] {
            new LocalModeTxTest().withStorage(StorageType.BINARY),
            new LocalModeTxTest().withStorage(StorageType.HEAP),
            new LocalModeTxTest().withStorage(StorageType.OFF_HEAP)
      };
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder configuration = getDefaultStandaloneCacheConfig(true);
      configuration.memory().storage(storage);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(configuration);
      cache = cm.getCache();
      return cm;
   }

   public LocalModeTxTest withStorage(StorageType storage) {
      this.storage = storage;
      return this;
   }

   @Override
   protected String parameters() {
      return "[storage=" + storage + "]";
   }

   public void testTxCommit1() throws Exception {
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      tm.begin();
      cache.put("key", "value");
      Transaction t = tm.suspend();
      assertTrue(cache.isEmpty());
      tm.resume(t);
      tm.commit();
      assertFalse(cache.isEmpty());
   }

   public void testTxCommit3() throws Exception {
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      tm.begin();
      cache.put("key", "value");
      tm.commit();
      assertFalse(cache.isEmpty());
   }

   public void testNonTx() throws Exception {
      cache.put("key", "value");
      assertFalse(cache.isEmpty());
   }

   public void testTxCommit2() throws Exception {
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      cache.put("key", "old");
      tm.begin();
      assertEquals("old", cache.get("key"));
      cache.put("key", "value");
      assertEquals("value", cache.get("key"));
      Transaction t = tm.suspend();
      assertEquals("old", cache.get("key"));
      tm.resume(t);
      tm.commit();
      assertEquals("value", cache.get("key"));
      assertFalse(cache.isEmpty());
   }

   public void testKeySet() throws Exception {
      tm().begin();
      cache.put("k1", "v1");
      cache.put("k2", "v2");
      assertEquals(2, cache.keySet().size());
      assertEquals(2, cache.values().size());
      tm().commit();
      assertEquals(2, cache.keySet().size());
      assertEquals(2, cache.values().size());
   }

   public void testKeySet2() throws Exception {
      cache.put("k1", "v1");
      cache.put("k2", "v2");
      assertEquals(2, cache.keySet().size());
      assertEquals(2, cache.values().size());
      tm().begin();
      assertEquals(2, cache.keySet().size());
      assertEquals(2, cache.values().size());
      cache.remove("k1");
      assertEquals(1, cache.keySet().size());
      assertEquals(1, cache.values().size());
      tm().rollback();
      assertEquals(2, cache.keySet().size());
      assertEquals(2, cache.values().size());
   }

   public void testKeySetAlterValue() throws Exception {
      cache.put("k1", "v1");
      cache.put("k2", "v2");
      assertEquals(2, cache.keySet().size());
      assertEquals(2, cache.values().size());
      tm().begin();
      assertEquals(2, cache.keySet().size());
      assertEquals(2, cache.values().size());
      cache.put("k1", "v3");
      assertEquals(2, cache.keySet().size());
      assertEquals(2, cache.values().size());
      assert cache.values().contains("v3");
      tm().rollback();
      assertEquals(2, cache.keySet().size());
      assertEquals(2, cache.values().size());
   }

   public void testTxCleanupWithKeySet() throws Exception {
      tm().begin();
      assertEquals(0, cache.keySet().size());
      TransactionTable txTable = getTransactionTable(cache);
      assertEquals(1, txTable.getLocalTransactions().size());
      tm().commit();
      assertEquals(0, txTable.getLocalTransactions().size());
   }

   public void testTxCleanupWithEntrySet() throws Exception {
      tm().begin();
      assertEquals(0, cache.entrySet().size());
      TransactionTable txTable = getTransactionTable(cache);
      assertEquals(1, txTable.getLocalTransactions().size());
      tm().commit();
      assertEquals(0, txTable.getLocalTransactions().size());
   }

   public void testTxCleanupWithValues() throws Exception {
      tm().begin();
      assertEquals(0, cache.values().size());
      TransactionTable txTable = getTransactionTable(cache);
      assertEquals(1, txTable.getLocalTransactions().size());
      tm().commit();
      assertEquals(0, txTable.getLocalTransactions().size());
   }

   public void testTxCleanupWithSize() throws Exception {
      tm().begin();
      assertEquals(0, cache.size());
      TransactionTable txTable = getTransactionTable(cache);
      assertEquals(1, txTable.getLocalTransactions().size());
      tm().commit();
      assertEquals(0, txTable.getLocalTransactions().size());
   }
}
