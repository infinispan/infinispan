package org.infinispan.tx;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

@Test(groups = "functional", sequential = true, testName = "tx.TransactionsSpanningCachesTestTest")
public class TransactionsSpanningCachesTest extends MultipleCacheManagersTest {

   protected StorageType storage1;
   protected StorageType storage2;

   public Object[] factory() {
      return new Object[] {
            new TransactionsSpanningCachesTest().withStorage(StorageType.HEAP, StorageType.HEAP),
            new TransactionsSpanningCachesTest().withStorage(StorageType.OFF_HEAP, StorageType.OFF_HEAP),
            new TransactionsSpanningCachesTest().withStorage(StorageType.HEAP, StorageType.OFF_HEAP)
      };
   }

   @Override
   protected void createCacheManagers() throws Exception {
      ConfigurationBuilder defaultCacheConfig = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      amendConfig(defaultCacheConfig);
      ConfigurationBuilder cb1 = defaultCacheConfig;
      ConfigurationBuilder cb2 = defaultCacheConfig;
      cb1.memory().storage(storage1);
      cb2.memory().storage(storage2);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createCacheManager(cb1);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createCacheManager(cb2);
      cm1.defineConfiguration("c1", cm1.getCache().getCacheConfiguration());
      cm2.defineConfiguration("c2", cm2.getCache().getCacheConfiguration());
      registerCacheManager(cm1, cm2);
   }

   protected void amendConfig(ConfigurationBuilder defaultCacheConfig) {
      //ignore
   }

   public TransactionsSpanningCachesTest withStorage(StorageType storage1, StorageType storage2) {
      this.storage1 = storage1;
      this.storage2 = storage2;
      return this;
   }

   @Override
   protected String parameters() {
      return "[storage1=" + storage1 + ", storage2=" + storage2 + "]";
   }

   public void testCommitSpanningCaches() throws Exception {
      Cache c1 = cacheManagers.get(0).getCache("c1");
      Cache c2 = cacheManagers.get(1).getCache("c2");

      assertTrue(c1.isEmpty());
      assertTrue(c2.isEmpty());

      c1.put("c1key", "c1value");
      c2.put("c2key", "c2value");

      assertFalse(c1.isEmpty());
      assertTrue(c1.size() == 1);
      assertEquals("c1value", c1.get("c1key"));

      assertFalse(c2.isEmpty());
      assertTrue(c2.size() == 1);
      assertEquals("c2value", c2.get("c2key"));

      TransactionManager tm = TestingUtil.getTransactionManager(c1);

      tm.begin();
      c1.put("c1key", "c1value_new");
      c2.put("c2key", "c2value_new");

      assertEquals("c1value_new", c1.get("c1key"));
      assertEquals("c2value_new", c2.get("c2key"));

      Transaction tx = tm.suspend();

      assertEquals("c1value", c1.get("c1key"));
      assertEquals("c2value", c2.get("c2key"));

      tm.resume(tx);
      tm.commit();

      assertEquals("c1value_new", c1.get("c1key"));
      assertEquals("c2value_new", c2.get("c2key"));
   }

   public void testRollbackSpanningCaches() throws Exception {
      Cache c1 = cacheManagers.get(0).getCache("c1");
      Cache c2 = cacheManagers.get(1).getCache("c2");

      assertTrue(c1.isEmpty());
      assertTrue(c2.isEmpty());

      c1.put("c1key", "c1value");
      c2.put("c2key", "c2value");

      assertFalse(c1.isEmpty());
      assertTrue(c1.size() == 1);
      assertEquals("c1value", c1.get("c1key"));

      assertFalse(c2.isEmpty());
      assertTrue(c2.size() == 1);
      assertEquals("c2value", c2.get("c2key"));

      TransactionManager tm = TestingUtil.getTransactionManager(c1);

      tm.begin();
      c1.put("c1key", "c1value_new");
      c2.put("c2key", "c2value_new");

      assertEquals("c1value_new", c1.get("c1key"));
      assertEquals("c2value_new", c2.get("c2key"));

      Transaction tx = tm.suspend();

      assertEquals("c1value", c1.get("c1key"));
      assertEquals("c2value", c2.get("c2key"));

      tm.resume(tx);
      tm.rollback();

      assertEquals("c1value", c1.get("c1key"));
      assertEquals("c2value", c2.get("c2key"));
   }
}
