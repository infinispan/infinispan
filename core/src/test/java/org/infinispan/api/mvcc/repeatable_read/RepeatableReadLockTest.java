package org.infinispan.api.mvcc.repeatable_read;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import jakarta.transaction.RollbackException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.api.mvcc.LockTestBase;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.mvcc.repeatable_read.RepeatableReadLockTest")
public class RepeatableReadLockTest extends LockTestBase {
   public RepeatableReadLockTest() {
      repeatableRead = true;
   }

   public void testRepeatableReadWithRemove() throws Exception {
      LockTestData tl = lockTestData;
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;
      cache.put("k", "v");

      tm.begin();
      assertNotNull(cache.get("k"));
      Transaction reader = tm.suspend();

      tm.begin();
      assertNotNull(cache.remove("k"));
      assertNull(cache.get("k"));
      tm.commit();

      assertNull(cache.get("k"));

      tm.resume(reader);
      assertNotNull(cache.get("k"));
      assertEquals("v", cache.get("k"));
      tm.commit();

      assertNull(cache.get("k"));
      assertNoLocks();
   }

   public void testRepeatableReadWithEvict() throws Exception {
      LockTestData tl = lockTestData;
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;

      cache.put("k", "v");

      tm.begin();
      assertNotNull(cache.get("k"));
      Transaction reader = tm.suspend();

      tm.begin();
      cache.evict("k");
      assertNull(cache.get("k"));
      tm.commit();

      assertNull(cache.get("k"));

      tm.resume(reader);
      assertNotNull(cache.get("k"));
      assertEquals("v", cache.get("k"));
      tm.commit();

      assertNull(cache.get("k"));
      assertNoLocks();
   }

   public void testRepeatableReadWithNull() throws Exception {
      LockTestData tl = lockTestData;
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;

      assertNull(cache.get("k"));

      tm.begin();
      assertNull(cache.get("k"));
      Transaction reader = tm.suspend();

      tm.begin();
      cache.put("k", "v");
      assertNotNull(cache.get("k"));
      assertEquals("v", cache.get("k"));
      tm.commit();

      assertNotNull(cache.get("k"));
      assertEquals("v", cache.get("k"));

      tm.resume(reader);
      assertEquals(null, cache.get("k"));
      tm.commit();

      assertNotNull(cache.get("k"));
      assertEquals("v", cache.get("k"));
      assertNoLocks();
   }

   public void testRepeatableReadWithNullRemoval() throws Exception {
      LockTestData tl = lockTestData;
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;

      // start with an empty cache
      tm.begin();
      cache.get("a");
      Transaction tx = tm.suspend();

      cache.put("a", "v2");
      assertEquals(cache.get("a"), "v2");

      tm.resume(tx);
      assertEquals(null, cache.get("a"));
      cache.remove("a");
      Exceptions.expectException(RollbackException.class, tm::commit);

      assertEquals(cache.get("a"), "v2");
   }

   @Override
   public void testLocksOnPutKeyVal() throws Exception {
      LockTestData tl = lockTestData;
      Cache<String, String> cache = tl.cache;
      EmbeddedTransactionManager tm = tl.tm;
      tm.begin();
      cache.put("k", "v");
      tm.getTransaction().runPrepare();
      assertLocked("k");
      tm.getTransaction().runCommit(false);
      tm.suspend();

      assertNoLocks();

      tm.begin();
      assertEquals(cache.get("k"), "v");


      assertNotLocked("k");
      tm.commit();

      assertNoLocks();

      tm.begin();
      cache.remove("k");
      tm.getTransaction().runPrepare();
      assertLocked("k");
      tm.getTransaction().runCommit(false);

      assertNoLocks();
   }

}
