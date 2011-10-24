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
package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionCoordinator;
import org.infinispan.transaction.TransactionTable;
import org.testng.annotations.Test;

@Test(testName = "lock.StaleLocksWithCommitDuringStateTransferTest", groups = "functional")
@CleanupAfterMethod
public class StaleLocksWithCommitDuringStateTransferTest extends MultipleCacheManagersTest {

   public static final int BLOCKING_CACHE_VIEW_ID = 1000;
   Cache<MagicKey, String> c1, c2;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration cfg = TestCacheManagerFactory.getDefaultConfiguration(true, Configuration.CacheMode.DIST_SYNC);
      cfg.setLockAcquisitionTimeout(100);
      cfg.setCacheStopTimeout(100);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      registerCacheManager(cm1, cm2);
      c1 = cm1.getCache();
      c2 = cm2.getCache();
   }

   public void testRollbackLocalFailure() throws Exception {
      doTest(false, true);
   }

   public void testCommitLocalFailure() throws Exception {
      doTest(true, true);
   }

   public void testRollbackRemoteFailure() throws Exception {
      doTest(false, false);
   }

   public void testCommitRemoteFailure() throws Exception {
      doTest(true, false);
   }

   private void doTest(boolean commit, boolean failOnOriginator) throws Exception {
      MagicKey k1 = new MagicKey(c1, "k1");
      MagicKey k2 = new MagicKey(c2, "k2");

      tm(c1).begin();
      c1.put(k1, "v1");
      c1.put(k2, "v2");

      // We split the transaction commit in two phases by calling the TransactionCoordinator methods directly
      TransactionTable txTable = TestingUtil.extractComponent(c1, TransactionTable.class);
      TransactionCoordinator txCoordinator = TestingUtil.extractComponent(c1, TransactionCoordinator.class);

      // Execute the prepare on both nodes
      LocalTransaction localTx = txTable.getLocalTransaction(tm(c1).getTransaction());
      txCoordinator.prepare(localTx);

      // Before calling commit we block transactions on one of the nodes to simulate a state transfer
      final StateTransferLock blockFirst = TestingUtil.extractComponent(failOnOriginator ? c1 : c2, StateTransferLock.class);
      final StateTransferLock blockSecond = TestingUtil.extractComponent(failOnOriginator ? c2 : c1, StateTransferLock.class);
      blockFirst.blockNewTransactions(1000);

      // Schedule the unblock on another thread since the main thread will be busy with the commit call
      Thread worker = new Thread("RehasherSim,StaleLocksWithCommitDuringStateTransferTest") {
         @Override
         public void run() {
            try {
               // should be much larger than the lock acquisition timeout
               Thread.sleep(1000);
               blockSecond.blockNewTransactions(BLOCKING_CACHE_VIEW_ID);
               blockFirst.unblockNewTransactions(BLOCKING_CACHE_VIEW_ID);
               blockSecond.unblockNewTransactions(BLOCKING_CACHE_VIEW_ID);
            } catch (InterruptedException e) {
               log.errorf(e, "Error blocking/unblocking transactions");
            }
         }
      };
      worker.start();

      try {
         // finally commit or rollback the transaction
         if (commit) {
            txCoordinator.commit(localTx, false);
         } else {
            txCoordinator.rollback(localTx);
         }

         // make the transaction manager forget about our tx so that we don't get rollback exceptions in the log
         tm(c1).suspend();
      } finally {
         // don't leak threads
         worker.join();
      }

      // test that we don't leak locks
      assertNotLocked(c1, k1);
      assertNotLocked(c2, k1);
      assertNotLocked(c1, k2);
      assertNotLocked(c2, k2);
   }
}

