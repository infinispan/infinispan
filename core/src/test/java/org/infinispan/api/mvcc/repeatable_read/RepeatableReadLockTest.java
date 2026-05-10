package org.infinispan.api.mvcc.repeatable_read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.infinispan.Cache;
import org.infinispan.api.mvcc.LockTestBase;
import org.infinispan.testing.Exceptions;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.testng.annotations.Test;

import jakarta.transaction.RollbackException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

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
      assertNull(cache.get("k"));
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
      assertEquals("v2", cache.get("a"));

      tm.resume(tx);
      assertNull(cache.get("a"));
      cache.remove("a");
      Exceptions.expectException(RollbackException.class, tm::commit);

      assertEquals("v2", cache.get("a"));
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
      assertEquals("v", cache.get("k"));


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
