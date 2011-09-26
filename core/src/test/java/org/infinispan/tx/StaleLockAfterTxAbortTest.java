/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.tx;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.InvocationContextContainerImpl;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

import javax.transaction.InvalidTransactionException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.concurrent.CountDownLatch;

/**
 * This tests the following pattern:
 * <p/>
 * Thread T1 holds lock for key K. TX1 attempts to lock a key K.  Waits for lock. TM times out the tx and aborts the tx
 * (calls XA.end, XA.rollback). T1 releases lock. Wait a few seconds. Make sure there are no stale locks (i.e., when the
 * thread for TX1 wakes up and gets the lock on K, it then releases and aborts).
 *
 * @author manik
 */
@Test (testName = "tx.StaleLockAfterTxAbortTest", groups = "unit")
public class StaleLockAfterTxAbortTest extends SingleCacheManagerTest {
   final String k = "key";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration c = getDefaultStandaloneConfig(true);
      return TestCacheManagerFactory.createCacheManager(c, true);
   }

   public void doTest() throws InterruptedException, SystemException, InvalidTransactionException {
      cache.put(k, "value"); // init value

      assertNotLocked(cache, k);

      InvocationContextContainerImpl icc = (InvocationContextContainerImpl) TestingUtil.extractComponent(cache, InvocationContextContainer.class);
      InvocationContext ctx = icc.getInvocationContext(true);
      LockManager lockManager = TestingUtil.extractComponent(cache, LockManager.class);
      lockManager.lockAndRecord(k, ctx);
      ctx.putLookedUpEntry(k, null);

      // test that the key is indeed locked.
      assertLocked(cache, k);
      final CountDownLatch txStartedLatch = new CountDownLatch(1);

      TxThread transactionThread = new TxThread(cache, txStartedLatch);

      transactionThread.start();
      txStartedLatch.countDown();
      Thread.sleep(500); // in case the thread needs some time to get to the locking code

      // now abort the tx.
      transactionThread.tm.resume(transactionThread.tx);
      transactionThread.tm.rollback();

      // now release the lock
      lockManager.releaseLocks(ctx);
      transactionThread.join();

      assertNotLocked(cache, k);
   }

   private class TxThread extends Thread {
      final Cache<Object, Object> cache;
      volatile Transaction tx;
      volatile Exception exception;
      final TransactionManager tm;
      final CountDownLatch txStartedLatch;

      private TxThread(Cache<Object, Object> cache, CountDownLatch txStartedLatch) {
         this.cache = cache;
         this.tx = null;
         this.tm = cache.getAdvancedCache().getTransactionManager();
         this.txStartedLatch = txStartedLatch;
      }

      @Override
      public void run() {
         try {
            // Now start a new tx.
            tm.begin();
            tx = tm.getTransaction();
            System.out.println("Started transaction " + tx);
            txStartedLatch.countDown();
            cache.put(k, "v2"); // this should block.
         } catch (Exception e) {
            exception = e;
         }
      }
   }

}
