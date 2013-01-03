/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.context;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.config.Configuration;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.testng.FileAssert.fail;

@Test(groups = {"functional"}, testName = "context.InvocationContextTest")
public class InvocationContextTest extends MultipleCacheManagersTest {
   private static final Log log = LogFactory.getLog(InvocationContextTest.class);

   public InvocationContextTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration cfg = TestCacheManagerFactory.getDefaultConfiguration(true);
      cfg.setSyncCommitPhase(true);
      cfg.setSyncRollbackPhase(true);
      cfg.fluent().transaction().lockingMode(LockingMode.PESSIMISTIC);
      createClusteredCaches(1, "timestamps", cfg);
   }

   public void testMishavingListenerResumesContext() {
      Cache cache = cache(0, "timestamps");
      cache.addListener(new CacheListener());
      try {
         cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).put("k", "v");
         fail("Should have failed with an exception");
      } catch (CacheException ce) {
         assert ce.getCause() instanceof RuntimeException;
      }
   }

   public void testThreadInterruptedDuringLocking() throws Throwable {
      final Cache cache = cache(0, "timestamps");
      cache.put("k", "v");
      // now acquire a lock on k so that subsequent threads will block
      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      tm.begin();
      cache.put("k", "v2");
      Transaction tx = tm.suspend();
      final List<Throwable> throwables = new LinkedList<Throwable>();

      Thread th = new Thread() {
         public void run() {
            try {
               cache.put("k", "v3");
            } catch (Throwable th) {
               throwables.add(th);
            }
         }
      };

      th.start();
      // th will now block trying to acquire the lock.
      th.interrupt();
      th.join();
      tm.resume(tx);
      tm.rollback();
      assert throwables.size() == 1;

      for (Throwable thr : throwables) thr.printStackTrace();
      assert throwables.get(0) instanceof CacheException;
      assert throwables.get(0).getCause() instanceof InterruptedException;
   }


   public void testThreadInterruptedAfterLocking() throws Throwable {
      final Cache cache = cache(0, "timestamps");
      cache.put("k", "v");
      CountDownLatch willTimeoutLatch = new CountDownLatch(1);
      CountDownLatch lockAquiredSignal = new CountDownLatch(1);
      DelayingListener dl = new DelayingListener(lockAquiredSignal, willTimeoutLatch);
      cache.addListener(dl);
      final List<Throwable> throwables = new LinkedList<Throwable>();

      Thread th = new Thread() {
         public void run() {
            try {
               cache.put("k", "v3");
            } catch (Throwable th) {
               throwables.add(th);
            }
         }
      };

      th.start();
      // wait for th to acquire the lock
      lockAquiredSignal.await();

      // and now interrupt the thread.
      th.interrupt();
      th.join();
      assert throwables.size() == 1;

      for (Throwable thr : throwables) thr.printStackTrace();
      assert throwables.get(0) instanceof CacheException;
   }

   @Listener
   public static class DelayingListener {
      CountDownLatch lockAcquiredLatch, waitLatch;

      public DelayingListener(CountDownLatch lockAcquiredLatch, CountDownLatch waitLatch) {
         this.lockAcquiredLatch = lockAcquiredLatch;
         this.waitLatch = waitLatch;
      }

      @CacheEntryModified
      public void entryModified(CacheEntryModifiedEvent event) {
         if (!event.isPre()) {
            lockAcquiredLatch.countDown();
            try {
               waitLatch.await();
            } catch (InterruptedException e) {
               throw new RuntimeException(e);
            }
         }
      }
   }

   @Listener
   public static class CacheListener {
      @CacheEntryModified
      public void entryModified(CacheEntryModifiedEvent event) {
         if (!event.isPre()) {
            log.debugf("Entry modified: %s, let's throw an exception!!", event);
            throw new RuntimeException("Testing exception handling");
         }
      }
   }
}
