package org.infinispan.api.mvcc.repeatable_read;

import org.infinispan.Cache;
import org.infinispan.api.mvcc.LockTestBase;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

@Test(groups = "functional", testName = "api.mvcc.repeatable_read.RepeatableReadLockTest")
public class RepeatableReadLockTest extends LockTestBase {
   public RepeatableReadLockTest() {
      repeatableRead = true;
   }

   public void testRepeatableReadWithRemove() throws Exception {
      LockTestBaseTL tl = threadLocal.get();
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;
      cache.put("k", "v");

      tm.begin();
      assert cache.get("k") != null;
      Transaction reader = tm.suspend();

      tm.begin();
      assert cache.remove("k") != null;
      assert cache.get("k") == null;
      tm.commit();

      assert cache.get("k") == null;

      tm.resume(reader);
      assert cache.get("k") != null;
      assert "v".equals(cache.get("k"));
      tm.commit();

      assert cache.get("k") == null;
      assertNoLocks();
   }

   public void testRepeatableReadWithEvict() throws Exception {
      LockTestBaseTL tl = threadLocal.get();
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;

      cache.put("k", "v");

      tm.begin();
      assert cache.get("k") != null;
      Transaction reader = tm.suspend();

      tm.begin();
      cache.evict("k");
      assert cache.get("k") == null;
      tm.commit();

      assert cache.get("k") == null;

      tm.resume(reader);
      assert cache.get("k") != null;
      assert "v".equals(cache.get("k"));
      tm.commit();

      assert cache.get("k") == null;
      assertNoLocks();
   }

   public void testRepeatableReadWithNull() throws Exception {
      LockTestBaseTL tl = threadLocal.get();
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;

      assert cache.get("k") == null;

      tm.begin();
      assert cache.get("k") == null;
      Transaction reader = tm.suspend();

      tm.begin();
      cache.put("k", "v");
      assert cache.get("k") != null;
      assert "v".equals(cache.get("k"));
      tm.commit();

      assert cache.get("k") != null;
      assert "v".equals(cache.get("k"));

      tm.resume(reader);
      Object o = cache.get("k");
      assert o == null : "found value " + o;
      tm.commit();

      assert cache.get("k") != null;
      assert "v".equals(cache.get("k"));
      assertNoLocks();
   }

   public void testRepeatableReadWithNullRemoval() throws Exception {
      LockTestBaseTL tl = threadLocal.get();
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;

      // start with an empty cache
      tm.begin();
      cache.get("a");
      Transaction tx = tm.suspend();

      cache.put("a", "v2");
      assert cache.get("a").equals("v2");

      tm.resume(tx);
      assert cache.get("a") == null : "expected null but received " + cache.get("a");
      cache.remove("a");
      tm.commit();

      assert cache.get("a") == null : "expected null but received " + cache.get("a");
   }

   public void testLocksOnPutKeyVal() throws Exception {
      LockTestBaseTL tl = threadLocal.get();
      Cache<String, String> cache = tl.cache;
      DummyTransactionManager tm = (DummyTransactionManager) tl.tm;
      tm.begin();
      cache.put("k", "v");
      tm.getTransaction().runPrepare();
      assertLocked("k");
      tm.getTransaction().runCommit(false);
      tm.suspend();

      assertNoLocks();

      tm.begin();
      assert cache.get("k").equals("v");


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
