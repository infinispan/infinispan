/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.api.mvcc;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.infinispan.util.AnyEquivalence;
import org.infinispan.util.Equivalence;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Collections;

/**
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 */
@Test(groups = "functional")
public abstract class LockTestBase extends AbstractInfinispanTest {
   private Log log = LogFactory.getLog(LockTestBase.class);
   protected boolean repeatableRead = true;
   protected boolean lockParentForChildInsertRemove = false;
   private CacheContainer cm;

   protected static final class LockTestBaseTL {
      public Cache<String, String> cache;
      public TransactionManager tm;
      public LockManager lockManager;
      public InvocationContextContainer icc;
   }

   protected final ThreadLocal<LockTestBaseTL> threadLocal = new ThreadLocal<LockTestBaseTL>();


   @BeforeMethod(alwaysRun = true)
   public void setUp() {
      LockTestBaseTL tl = new LockTestBaseTL();
      Configuration defaultCfg = new Configuration();
      defaultCfg.setIsolationLevel(repeatableRead ? IsolationLevel.REPEATABLE_READ : IsolationLevel.READ_COMMITTED);
      defaultCfg.setLockAcquisitionTimeout(200); // 200 ms
      defaultCfg.fluent().transaction().transactionManagerLookup(new DummyTransactionManagerLookup());
      cm = TestCacheManagerFactory.createCacheManager(defaultCfg);
      tl.cache = cm.getCache();
      tl.lockManager = TestingUtil.extractComponentRegistry(tl.cache).getComponent(LockManager.class);
      tl.icc = TestingUtil.extractComponentRegistry(tl.cache).getComponent(InvocationContextContainer.class);
      tl.tm = TestingUtil.extractComponentRegistry(tl.cache).getComponent(TransactionManager.class);
      threadLocal.set(tl);
   }

   @AfterMethod(alwaysRun = true)
   public void tearDown() {
      LockTestBaseTL tl = threadLocal.get();
      log.debug("**** - STARTING TEARDOWN - ****");
      TestingUtil.killCacheManagers(cm);
      threadLocal.set(null);
   }

   protected void assertLocked(Object key) {
      LockTestBaseTL tl = threadLocal.get();
      LockAssert.assertLocked(key, tl.lockManager, tl.icc);
   }

   protected void assertNotLocked(Object key) {
      LockTestBaseTL tl = threadLocal.get();
      LockAssert.assertNotLocked(key, tl.icc);
   }

   protected void assertNoLocks() {
      LockTestBaseTL tl = threadLocal.get();
      LockAssert.assertNoLocks(tl.lockManager, tl.icc);
   }

   public void testLocksOnPutKeyVal() throws Exception {
      LockTestBaseTL tl = threadLocal.get();
      Cache<String, String> cache = tl.cache;
      DummyTransactionManager tm = (DummyTransactionManager) tl.tm;
      tm.begin();
      cache.put("k", "v");
      assert tm.getTransaction().runPrepare();
      assertLocked("k");
      tm.getTransaction().runCommitTx();
      tm.suspend();

      assertNoLocks();

      tm.begin();
      assert cache.get("k").equals("v");
      assertNotLocked("k");
      tm.commit();

      assertNoLocks();

      tm.begin();
      cache.remove("k");
      assert tm.getTransaction().runPrepare();
      assertLocked("k");
      tm.getTransaction().runCommitTx();

      assertNoLocks();
   }

   public void testLocksOnPutData() throws Exception {
      LockTestBaseTL tl = threadLocal.get();
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;
      tm.begin();
      cache.putAll(Collections.singletonMap("k", "v"));
      assert "v".equals(cache.get("k"));
      final DummyTransaction tx = ((DummyTransactionManager) tm).getTransaction();
      assert tx.runPrepare();
      assertLocked("k");
      tx.runCommitTx();
      assertNoLocks();
      tm.suspend();

      tm.begin();
      assert "v".equals(cache.get("k"));
      assertNoLocks();
      tm.commit();

      assertNoLocks();
   }

   public void testLocksOnEvict() throws Exception {
      LockTestBaseTL tl = threadLocal.get();
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;
      // init some data
      cache.putAll(Collections.singletonMap("k", "v"));

      assert "v".equals(cache.get("k"));

      tm.begin();
      cache.evict("k");
      assertNotLocked("k");
      tm.commit();
      assert !cache.containsKey("k") : "Should not exist";
      assertNoLocks();
   }

   public void testLocksOnRemoveNonexistent() throws Exception {
      LockTestBaseTL tl = threadLocal.get();
      Cache<String, String> cache = tl.cache;
      DummyTransactionManager tm = (DummyTransactionManager) tl.tm;
      assert !cache.containsKey("k") : "Should not exist";

      tm.begin();
      cache.remove("k");
      tm.getTransaction().runPrepare();
      assertLocked("k");
      tm.getTransaction().runCommitTx();

      assert !cache.containsKey("k") : "Should not exist";
      assertNoLocks();
   }

   public void testLocksOnEvictNonexistent() throws Exception {
      LockTestBaseTL tl = threadLocal.get();
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
      LockTestBaseTL tl = threadLocal.get();
      Cache<String, String> cache = tl.cache;
      DummyTransactionManager tm = (DummyTransactionManager) tl.tm;
      // init some data
      cache.put("k", "v");
      cache.put("k2", "v2");

      assert "v".equals(cache.get("k"));
      assert "v2".equals(cache.get("k2"));

      // remove
      tm.begin();
      cache.clear();
      assert tm.getTransaction().runPrepare();

      assertLocked("k");
      assertLocked("k2");
      tm.getTransaction().runCommitTx();

      assert cache.isEmpty();
      assertNoLocks();
   }

   public void testWriteDoesntBlockRead() throws Exception {
      LockTestBaseTL tl = threadLocal.get();
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;
      cache.put("k", "v");

      // start a write.
      tm.begin();
      cache.put("k2", "v2");
      Transaction write = tm.suspend();

      // now start a read and confirm that the write doesn't block it.
      tm.begin();
      assert "v".equals(cache.get("k"));
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
         assert "v2".equals(cache.get("k2")) : "Read committed should see committed changes";
      tm.commit();
      assertNoLocks();
   }

   public void testUpdateDoesntBlockRead() throws Exception {
      LockTestBaseTL tl = threadLocal.get();
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;
      cache.put("k", "v");

      // Change K
      tm.begin();
      cache.put("k", "v2");
      Transaction write = tm.suspend();

      // now start a read and confirm that the write doesn't block it.
      tm.begin();
      assert "v".equals(cache.get("k"));
      Transaction read = tm.suspend();

      // commit the write
      tm.resume(write);
      tm.commit();

      assertNoLocks();

      tm.resume(read);
      if (repeatableRead)
         assert "v".equals(cache.get("k")) : "Should have repeatable read";
      else
         assert "v2".equals(cache.get("k")) : "Read committed should see committed changes";
      tm.commit();
      assertNoLocks();
   }


   public void testWriteDoesntBlockReadNonexistent() throws Exception {
      LockTestBaseTL tl = threadLocal.get();
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
         assert "v".equals(cache.get("k")) : "Read committed should see committed changes";
      }
      tm.commit();
      assertNoLocks();
   }

   public void testConcurrentWriters() throws Exception {
      LockTestBaseTL tl = threadLocal.get();
      Cache<String, String> cache = tl.cache;
      DummyTransactionManager tm = (DummyTransactionManager) tl.tm;
      tm.begin();
      cache.put("k", "v");
      final DummyTransaction transaction = tm.getTransaction();
      assert transaction.runPrepare();
      tm.suspend();

      tm.begin();
      cache.put("k", "v");
      assert !tm.getTransaction().runPrepare();

      tm.rollback();
      tm.resume(transaction);
      transaction.runCommitTx();

      assertNoLocks();
   }

   public void testRollbacks() throws Exception {
      LockTestBaseTL tl = threadLocal.get();
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;
      cache.put("k", "v");
      tm.begin();
      assert "v".equals(cache.get("k"));
      Transaction reader = tm.suspend();

      tm.begin();
      cache.put("k", "v2");
      tm.rollback();

      tm.resume(reader);
      Object value = cache.get("k");
      assert "v".equals(value) : "Expecting 'v' but was " + value;
      tm.commit();

      // even after commit
      assert "v".equals(cache.get("k"));
      assertNoLocks();
   }

   public void testRollbacksOnNullEntry() throws Exception {
      LockTestBaseTL tl = threadLocal.get();
      Cache<String, String> cache = tl.cache;
      TransactionManager tm = tl.tm;
      tm.begin();
      assert null == cache.get("k");
      Transaction reader = tm.suspend();

      tm.begin();
      cache.put("k", "v");
      assert "v".equals(cache.get("k"));
      tm.rollback();

      tm.resume(reader);
      assert null == cache.get("k") : "Expecting null but was " + cache.get("k");
      tm.commit();

      // even after commit
      assert null == cache.get("k");
      assertNoLocks();
   }
}
