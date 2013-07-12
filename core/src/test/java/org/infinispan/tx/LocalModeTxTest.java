package org.infinispan.tx;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

@Test(groups = "functional", testName = "tx.LocalModeTxTest")
public class LocalModeTxTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder configuration = getDefaultStandaloneCacheConfig(true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(configuration);
      cache = cm.getCache();
      return cm;
   }

   public void testTxCommit1() throws Exception {
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      tm.begin();
      cache.put("key", "value");
      Transaction t = tm.suspend();
      assert cache.isEmpty();
      tm.resume(t);
      tm.commit();
      assert !cache.isEmpty();
   }

   public void testTxCommit3() throws Exception {
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      tm.begin();
      cache.put("key", "value");
      tm.commit();
      assert !cache.isEmpty();
   }

   public void testNonTx() throws Exception {
      cache.put("key", "value");
      assert !cache.isEmpty();
   }

   public void testTxCommit2() throws Exception {
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      cache.put("key", "old");
      tm.begin();
      assert cache.get("key").equals("old");
      cache.put("key", "value");
      assert cache.get("key").equals("value");
      Transaction t = tm.suspend();
      assert cache.get("key").equals("old");
      tm.resume(t);
      tm.commit();
      assert cache.get("key").equals("value");
      assert !cache.isEmpty();
   }

   public void testKeySet() throws Exception {
      tm().begin();
      cache.put("k1", "v1");
      cache.put("k2", "v2");
      assert cache.keySet().size() == 2;
      assert cache.values().size() == 2;
      tm().commit();
      assert cache.keySet().size() == 2;
      assert cache.values().size() == 2;
   }

   public void testKeySet2() throws Exception {
      cache.put("k1", "v1");
      cache.put("k2", "v2");
      assert cache.keySet().size() == 2;
      assert cache.values().size() == 2;
      tm().begin();
      assert cache.keySet().size() == 2;
      assert cache.values().size() == 2;
      cache.remove("k1");
      assert cache.keySet().size() == 1;
      assert cache.values().size() == 1;
      tm().rollback();
      assert cache.keySet().size() == 2;
      assert cache.values().size() == 2;
   }

   public void testKeySetAlterValue() throws Exception {
      cache.put("k1", "v1");
      cache.put("k2", "v2");
      assert cache.keySet().size() == 2;
      assert cache.values().size() == 2;
      tm().begin();
      assert cache.keySet().size() == 2;
      assert cache.values().size() == 2;
      cache.put("k1", "v3");
      assert cache.keySet().size() == 2;
      assert cache.values().size() == 2;
      assert cache.values().contains("v3");
      tm().rollback();
      assert cache.keySet().size() == 2;
      assert cache.values().size() == 2;
   }
}
