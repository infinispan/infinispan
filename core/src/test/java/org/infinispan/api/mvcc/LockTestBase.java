package org.infinispan.api.mvcc;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 */
@Test(groups = "functional")
public abstract class LockTestBase extends AbstractInfinispanTest {
   private Log log = LogFactory.getLog(LockTestBase.class);
   protected boolean repeatableRead = true;
   private CacheContainer cm;

   protected static final class LockTestData {
      public Cache<String, String> cache;
      public TransactionManager tm;
      public LockManager lockManager;
   }

   protected LockTestData lockTestData;


   @BeforeMethod
   public void setUp() {
      LockTestData ltd = new LockTestData();
      ConfigurationBuilder defaultCfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);

      defaultCfg
         .locking()
            .isolationLevel(repeatableRead ? IsolationLevel.REPEATABLE_READ : IsolationLevel.READ_COMMITTED)
            .lockAcquisitionTimeout(200)
            .transaction()
               .transactionManagerLookup(new DummyTransactionManagerLookup());
      cm = TestCacheManagerFactory.createCacheManager(defaultCfg);
      ltd.cache = cm.getCache();
      ltd.lockManager = TestingUtil.extractComponentRegistry(ltd.cache).getComponent(LockManager.class);
      ltd.tm = TestingUtil.extractComponentRegistry(ltd.cache).getComponent(TransactionManager.class);
      lockTestData = ltd;
   }

   @AfterMethod
   public void tearDown() {
      log.debug("**** - STARTING TEARDOWN - ****");
      TestingUtil.killCacheManagers(cm);
      lockTestData = null;
   }

   protected void assertLocked(Object key) {
      LockAssert.assertLocked(key, lockTestData.lockManager);
   }

   protected void assertNotLocked(Object key) {
      LockAssert.assertNotLocked(key, lockTestData.lockManager);
   }

   protected void assertNoLocks() {
      LockAssert.assertNoLocks(lockTestData.lockManager);
   }

   public void testLocksOnPutKeyVal() throws Exception {
      Cache<String, String> cache = lockTestData.cache;
      DummyTransactionManager tm = (DummyTransactionManager) lockTestData.tm;
      tm.begin();
      cache.put("k", "v");
      assertTrue(tm.getTransaction().runPrepare());
      assertLocked("k");
      tm.getTransaction().runCommit(false);

      assertNoLocks();

      tm.begin();
      assertEquals("v", cache.get("k"));
      assertNotLocked("k");
      tm.commit();

      assertNoLocks();

      tm.begin();
      cache.remove("k");
      assertTrue(tm.getTransaction().runPrepare());
      assertLocked("k");
      tm.getTransaction().runCommit(false);

      assertNoLocks();
   }

   public void testLocksOnPutData() throws Exception {
      LockTestData tl = lockTestData;
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;
      tm.begin();
      cache.putAll(Collections.singletonMap("k", "v"));
      assertEquals("v", cache.get("k"));
      final DummyTransaction tx = ((DummyTransactionManager) tm).getTransaction();
      assertTrue(tx.runPrepare());
      assertLocked("k");
      tx.runCommit(false);
      assertNoLocks();

      tm.begin();
      assertEquals("v", cache.get("k"));
      assertNoLocks();
      tm.commit();

      assertNoLocks();
   }

   public void testLocksOnEvict() throws Exception {
      LockTestData tl = lockTestData;
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;
      // init some data
      cache.putAll(Collections.singletonMap("k", "v"));

      assertEquals("v", cache.get("k"));

      tm.begin();
      cache.evict("k");
      assertNotLocked("k");
      tm.commit();
      assertFalse(cache.containsKey("k"));
      assertNoLocks();
   }

   public void testLocksOnRemoveNonexistent() throws Exception {
      LockTestData tl = lockTestData;
      Cache<String, String> cache = tl.cache;
      DummyTransactionManager tm = (DummyTransactionManager) tl.tm;
      assert !cache.containsKey("k") : "Should not exist";

      tm.begin();
      cache.remove("k");
      tm.getTransaction().runPrepare();
      assertLocked("k");
      tm.getTransaction().runCommit(false);

      assert !cache.containsKey("k") : "Should not exist";
      assertNoLocks();
   }

   public void testLocksOnEvictNonexistent() throws Exception {
      LockTestData tl = lockTestData;
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;
      assert !cache.containsKey("k") : "Should not exist";

      tm.begin();
      cache.evict("k");
      assertNotLocked("k");
      tm.commit();
      assert !cache.containsKey("k") : "Should not exist";
      assertNoLocks();
   }

   public void testLocksOnRemoveData() throws Exception {
      LockTestData tl = lockTestData;
      Cache<String, String> cache = tl.cache;
      DummyTransactionManager tm = (DummyTransactionManager) tl.tm;
      // init some data
      cache.put("k", "v");
      cache.put("k2", "v2");

      assertEquals("v", cache.get("k"));
      assertEquals("v2", cache.get("k2"));

      // remove
      tm.begin();
      cache.remove("k");
      cache.remove("k2");
      assertTrue(tm.getTransaction().runPrepare());

      assertLocked("k");
      assertLocked("k2");
      tm.getTransaction().runCommit(false);

      assert cache.isEmpty();
      assertNoLocks();
   }

   public void testWriteDoesntBlockRead() throws Exception {
      LockTestData tl = lockTestData;
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;
      cache.put("k", "v");

      // start a write.
      tm.begin();
      cache.put("k2", "v2");
      Transaction write = tm.suspend();

      // now start a read and confirm that the write doesn't block it.
      tm.begin();
      assertEquals("v", cache.get("k"));
      assert null == cache.get("k2") : "Should not see uncommitted changes";
      Transaction read = tm.suspend();

      // commit the write
      tm.resume(write);
      tm.commit();

      assertNoLocks();

      tm.resume(read);
      if (repeatableRead)
         assert null == cache.get("k2") : "Should have repeatable read";
      else
         assertEquals("Read committed should see committed changes", "v2", cache.get("k2"));
      tm.commit();
      assertNoLocks();
   }

   public void testUpdateDoesntBlockRead() throws Exception {
      LockTestData tl = lockTestData;
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;
      cache.put("k", "v");

      // Change K
      tm.begin();
      cache.put("k", "v2");
      Transaction write = tm.suspend();

      // now start a read and confirm that the write doesn't block it.
      tm.begin();
      assertEquals("v", cache.get("k"));
      Transaction read = tm.suspend();

      // commit the write
      tm.resume(write);
      tm.commit();

      assertNoLocks();

      tm.resume(read);
      if (repeatableRead)
         assertEquals("Should have repeatable read", "v", cache.get("k"));
      else
         assertEquals("Read committed should see committed changes", "v2", cache.get("k"));
      tm.commit();
      assertNoLocks();
   }


   public void testWriteDoesntBlockReadNonexistent() throws Exception {
      LockTestData tl = lockTestData;
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;
      // start a write.
      tm.begin();
      cache.put("k", "v");
      Transaction write = tm.suspend();

      // now start a read and confirm that the write doesn't block it.
      tm.begin();
      assert null == cache.get("k") : "Should not see uncommitted changes";
      Transaction read = tm.suspend();

      // commit the write
      tm.resume(write);
      tm.commit();

      assertNoLocks();

      tm.resume(read);
      if (repeatableRead) {
         assert null == cache.get("k") : "Should have repeatable read";
      } else {
         assertEquals("Read committed should see committed changes", "v", cache.get("k"));
      }
      tm.commit();
      assertNoLocks();
   }

   public void testConcurrentWriters() throws Exception {
      LockTestData tl = lockTestData;
      Cache<String, String> cache = tl.cache;
      DummyTransactionManager tm = (DummyTransactionManager) tl.tm;
      tm.begin();
      cache.put("k", "v");
      final DummyTransaction transaction = tm.getTransaction();
      assertTrue(transaction.runPrepare());
      tm.suspend();

      tm.begin();
      cache.put("k", "v");
      assert !tm.getTransaction().runPrepare();

      tm.rollback();
      tm.resume(transaction);
      transaction.runCommit(false);

      assertNoLocks();
   }

   public void testRollbacks() throws Exception {
      LockTestData tl = lockTestData;
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;
      cache.put("k", "v");
      tm.begin();
      assertEquals("v", cache.get("k"));
      Transaction reader = tm.suspend();

      tm.begin();
      cache.put("k", "v2");
      tm.rollback();

      tm.resume(reader);
      Object value = cache.get("k");
      assertEquals("v", value);
      tm.commit();

      // even after commit
      assertEquals("v", cache.get("k"));
      assertNoLocks();
   }

   public void testRollbacksOnNullEntry() throws Exception {
      LockTestData tl = lockTestData;
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;
      tm.begin();
      assert null == cache.get("k");
      Transaction reader = tm.suspend();

      tm.begin();
      cache.put("k", "v");
      assertEquals("v", cache.get("k"));
      tm.rollback();

      tm.resume(reader);
      assert null == cache.get("k") : "Expecting null but was " + cache.get("k");
      tm.commit();

      // even after commit
      assert null == cache.get("k");
      assertNoLocks();
   }
}
