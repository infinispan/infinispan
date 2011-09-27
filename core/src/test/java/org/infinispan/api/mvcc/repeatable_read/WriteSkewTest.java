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
package org.infinispan.api.mvcc.repeatable_read;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.api.mvcc.LockAssert;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

@Test(groups = "functional", testName = "api.mvcc.repeatable_read.WriteSkewTest")
public class WriteSkewTest extends AbstractInfinispanTest {
   private static final Log log = LogFactory.getLog(WriteSkewTest.class);
   protected TransactionManager tm;
   protected LockManager lockManager;
   protected InvocationContextContainer icc;
   protected EmbeddedCacheManager cacheManager;
   protected Cache cache;

   @BeforeTest
   public void setUp() {
      Configuration c = new Configuration();
      c.fluent().transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      c.setLockAcquisitionTimeout(200);
      c.setIsolationLevel(IsolationLevel.REPEATABLE_READ);
      cacheManager = TestCacheManagerFactory.createCacheManager(c);
   }

   @AfterTest
   public void tearDown() {
      TestingUtil.killCacheManagers(cacheManager);
      cacheManager = null;
      cache =null;
      lockManager = null;
      tm = null;
      icc = null;
   }

   private void postStart() {
      lockManager = TestingUtil.extractComponentRegistry(cache).getComponent(LockManager.class);
      icc = TestingUtil.extractComponentRegistry(cache).getComponent(InvocationContextContainer.class);
      tm = TestingUtil.extractComponentRegistry(cache).getComponent(TransactionManager.class);
   }

   protected void assertNoLocks() {
      LockAssert.assertNoLocks(lockManager, icc);
   }

   public void testDontCheckWriteSkew() throws Exception {
      Configuration noWriteSkewCheck = new Configuration();
      noWriteSkewCheck.setWriteSkewCheck(false);
      cacheManager.defineConfiguration("noWriteSkewCheck", noWriteSkewCheck);
      cache = cacheManager.getCache("noWriteSkewCheck");
      postStart();
      doTest(true);
   }

   public void testCheckWriteSkew() throws Exception {
      Configuration writeSkewCheck = new Configuration();
      writeSkewCheck.setWriteSkewCheck(true);
      cacheManager.defineConfiguration("writeSkewCheck", writeSkewCheck);
      cache = cacheManager.getCache("writeSkewCheck");
      postStart();
      doTest(false);
   }

   public void testWriteSkewWithOnlyPut() throws Exception {
      Configuration writeSkewCheck = new Configuration();
      writeSkewCheck.setWriteSkewCheck(true);
      cacheManager.defineConfiguration("writeSkewCheckWithOnlyPut", writeSkewCheck);
      cache = cacheManager.getCache("writeSkewCheckWithOnlyPut");
      postStart();

      tm.begin();
      try {
         cache.put("k", "init");
      } catch (Exception e) {
         tm.setRollbackOnly();
         throw e;
      } finally {
         if (tm.getStatus() == Status.STATUS_ACTIVE) tm.commit();
         else tm.rollback();
      }

      int nbWriters = 10;
      CyclicBarrier barrier = new CyclicBarrier(nbWriters + 1);
      List<Future<Void>> futures = new ArrayList<Future<Void>>(nbWriters);
      ExecutorService executorService = Executors.newCachedThreadPool(new ThreadFactory() {
         volatile int i = 0;
         @Override
         public Thread newThread(Runnable r) {
            int ii = i++;
            return new Thread(r, "EntryWriter-" + ii + ", WriteSkewTest");
         }
      });

      try {
         for (int i = 0; i < nbWriters; i++) {
            log.debug("Schedule execution");
            Future<Void> future = executorService.submit(new EntryWriter(barrier));
            futures.add(future);
         }
         barrier.await(); // wait for all threads to be ready
         barrier.await(); // wait for all threads to finish

         log.debug("All threads finished, let's shutdown the executor and check whether any exceptions were reported");
         for (Future<Void> future : futures) future.get();
      } finally {
         executorService.shutdownNow();
      }
   }


   private void doTest(final boolean allowWriteSkew) throws Exception {
      cache.put("k", "v");
      final Set<Exception> w1exceptions = new HashSet<Exception>();
      final Set<Exception> w2exceptions = new HashSet<Exception>();
      final CountDownLatch w1Signal = new CountDownLatch(1);
      final CountDownLatch w2Signal = new CountDownLatch(1);
      final CountDownLatch threadSignal = new CountDownLatch(2);

      Thread w1 = new Thread("Writer-1, WriteSkewTest") {
         public void run() {
            boolean didCoundDown = false;
            try {
               tm.begin();
               assert "v".equals(cache.get("k"));
               threadSignal.countDown();
               didCoundDown = true;
               w1Signal.await();
               cache.put("k", "v2");
               tm.commit();
            }
            catch (Exception e) {
               w1exceptions.add(e);
            }
            finally {
               if (!didCoundDown) threadSignal.countDown();
            }
         }
      };

      Thread w2 = new Thread("Writer-2, WriteSkewTest") {
         public void run() {
            boolean didCoundDown = false;
            try {
               tm.begin();
               assert "v".equals(cache.get("k"));
               threadSignal.countDown();
               didCoundDown = true;
               w2Signal.await();
               cache.put("k", "v3");
               tm.commit();
            }
            catch (Exception e) {
               w2exceptions.add(e);
               // the exception will be thrown when doing a cache.put().  We should make sure we roll back the tx to release locks.
               if (!allowWriteSkew) {
                  try {
                     tm.rollback();
                  }
                  catch (SystemException e1) {
                     // do nothing.
                  }
               }
            }
            finally {
               if (!didCoundDown) threadSignal.countDown();
            }
         }
      };

      w1.start();
      w2.start();

      threadSignal.await();
      // now.  both txs have read.
      // let tx1 start writing
      w1Signal.countDown();
      w1.join();

      w2Signal.countDown();
      w2.join();

      if (allowWriteSkew) {
         // should have no exceptions!!
         throwExceptions(w1exceptions, w2exceptions);
         assert w2exceptions.isEmpty();
         assert w1exceptions.isEmpty();
         assert "v3".equals(cache.get("k")) : "W2 should have overwritten W1's work!";

         assertNoLocks();
      } else {
         Collection<Exception> combined = new HashSet<Exception>(w1exceptions);
         combined.addAll(w2exceptions);
         assert !combined.isEmpty();
         assert combined.size() == 1;
         assert combined.iterator().next() instanceof CacheException;
      }
   }

   private void throwExceptions(Collection<Exception>... exceptions) throws Exception {
      for (Collection<Exception> ce : exceptions) {
         for (Exception e : ce) throw e;
      }
   }

   protected class EntryWriter implements Callable<Void> {
      private final CyclicBarrier barrier;

      public EntryWriter(CyclicBarrier barrier) {
         this.barrier = barrier;
      }

      public Void call() throws Exception {
         try {
            log.debug("Wait for all executions paths to be ready to perform calls");
            barrier.await();

            tm.begin();
            try {
               cache.put("k", "_lockthisplease_");
            } catch (Exception e) {
               log.error("Unexpected", e);
               tm.setRollbackOnly();
               throw e;
            } finally {
               if (tm.getStatus() == Status.STATUS_ACTIVE) tm.commit();
               else tm.rollback();
            }

            return null;
         } finally {
            log.debug("Wait for all execution paths to finish");
            barrier.await();
         }
      }
   }
}
