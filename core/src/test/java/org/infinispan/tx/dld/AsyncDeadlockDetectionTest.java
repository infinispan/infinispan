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
package org.infinispan.tx.dld;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.PerCacheExecutorThread;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 * Tests deadlock detection for async caches.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "tx.AsyncDeadlockDetectionTest")
public class AsyncDeadlockDetectionTest extends MultipleCacheManagersTest {
   private PerCacheExecutorThread t0;
   private PerCacheExecutorThread t1;
   private RemoteReplicationInterceptor remoteReplicationInterceptor;


   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.REPL_ASYNC, true);
      config.setEnableDeadlockDetection(true);
      config.setSyncCommitPhase(true);
      config.setSyncRollbackPhase(true);
      config.setUseLockStriping(false);
      assert config.isEnableDeadlockDetection();
      createClusteredCaches(2, "test", config);
      assert config.isEnableDeadlockDetection();

      
      remoteReplicationInterceptor = new RemoteReplicationInterceptor();
      Cache cache0 = cache(0, "test");
      Cache cache1 = cache(1, "test");
      cache1.getAdvancedCache().addInterceptor(remoteReplicationInterceptor, 0);
      assert cache0.getConfiguration().isEnableDeadlockDetection();
      assert cache1.getConfiguration().isEnableDeadlockDetection();
      assert !cache0.getConfiguration().isExposeJmxStatistics();
      assert !cache1.getConfiguration().isExposeJmxStatistics();

      ((DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache0)).setExposeJmxStats(true);
      ((DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache1)).setExposeJmxStats(true);
   }

   @BeforeMethod
   public void beforeMethod() {
      Cache cache0 = cache(0, "test");
      Cache cache1 = cache(1, "test");
      t0 = new PerCacheExecutorThread(cache0, 0);
      t1 = new PerCacheExecutorThread(cache1, 1);
   }

   @AfterMethod
   public void afterMethod() {
      Cache cache0 = cache(0, "test");
      Cache cache1 = cache(1, "test");
      t0.stopThread();
      t1.stopThread();
      ((DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache0)).resetStatistics();
      ((DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache1)).resetStatistics();
      remoteReplicationInterceptor.executionResponse = null;
      remoteReplicationInterceptor = null;
      t0 = null;
      t1 = null;
   }

   public void testRemoteTxVsLocal() throws Exception {
      Cache cache0 = cache(0, "test");
      Cache cache1 = cache(1, "test");
      assertEquals(PerCacheExecutorThread.OperationsResult.BEGGIN_TX_OK, t0.execute(PerCacheExecutorThread.Operations.BEGGIN_TX));
      t0.setKeyValue("k1", "v1_t0");
      assertEquals(PerCacheExecutorThread.OperationsResult.PUT_KEY_VALUE_OK, t0.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE));
      t0.setKeyValue("k2", "v2_t0");
      assertEquals(PerCacheExecutorThread.OperationsResult.PUT_KEY_VALUE_OK, t0.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE));

      assertEquals(PerCacheExecutorThread.OperationsResult.BEGGIN_TX_OK, t1.execute(PerCacheExecutorThread.Operations.BEGGIN_TX));
      t1.setKeyValue("k2", "v2_t1");
      assertEquals(PerCacheExecutorThread.OperationsResult.PUT_KEY_VALUE_OK, t1.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE));

      t0.execute(PerCacheExecutorThread.Operations.COMMIT_TX);

      final LockManager lm1 = TestingUtil.extractLockManager(cache1);

      eventually(new Condition() {
         public boolean isSatisfied() throws Exception {
            return lm1.isLocked("k1");
         }
      }); //now t0 replicated, acquired lock on k1 and it tries to acquire lock on k2


      t1.setKeyValue("k1", "v1_t1");
      t1.executeNoResponse(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);


      Object t1Response = t1.waitForResponse();
      Object t0Response = remoteReplicationInterceptor.getResponse();

      log.trace("t0Response = " + t0Response);
      log.trace("t1Response = " + t1Response);

      assert xor(t1Response instanceof DeadlockDetectedException, t0Response instanceof DeadlockDetectedException);
      TransactionTable transactionTable1 = TestingUtil.extractComponent(cache1, TransactionTable.class);


      if (t0Response instanceof DeadlockDetectedException) {
         replListener(cache0).expectWithTx(PutKeyValueCommand.class, PutKeyValueCommand.class);
         assertEquals(t1.execute(PerCacheExecutorThread.Operations.COMMIT_TX), PerCacheExecutorThread.OperationsResult.COMMIT_TX_OK);
         replListener(cache0).waitForRpc();
         assertEquals(transactionTable1.getLocalTxCount(), 0);
      }

      DeadlockDetectingLockManager ddLm0 = (DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache0);
      DeadlockDetectingLockManager ddLm1 = (DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache1);
      
      assertFalse(ddLm0.isLocked("k1"));
      assertFalse(ddLm1.isLocked("k1"));
      assertFalse(ddLm0.isLocked("k2"));
      assertFalse(ddLm1.isLocked("k2"));
      TransactionTable transactionTable0 = TestingUtil.extractComponent(cache0, TransactionTable.class);
      assertEquals(transactionTable0.getLocalTxCount(), 0);
      for (int i = 0; i < 20; i++) {
         if (!(transactionTable0.getRemoteTxCount() == 0)) Thread.sleep(50);
      }

      assertEquals(transactionTable0.getRemoteTxCount(), 0);

      for (int i = 0; i < 20; i++) {
         if (!(transactionTable1.getRemoteTxCount() == 0)) Thread.sleep(50);
      }
      assertEquals(transactionTable1.getRemoteTxCount(), 0);

      if (t1Response instanceof DeadlockDetectedException) {
         assertEquals(cache0.get("k1"), "v1_t0");
         assertEquals(cache0.get("k2"), "v2_t0");
         assertEquals(cache1.get("k1"), "v1_t0");
         assertEquals(cache1.get("k2"), "v2_t0");
      } else {
         assertEquals(cache0.get("k1"), "v1_t1");
         assertEquals(cache0.get("k2"), "v2_t1");
         assertEquals(cache1.get("k1"), "v1_t1");
         assertEquals(cache1.get("k2"), "v2_t1");
      }
   }


   public static class RemoteReplicationInterceptor extends CommandInterceptor {

      public volatile Object executionResponse;

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         try {
            return invokeNextInterceptor(ctx, command);
         } catch (Throwable throwable) {
            if (!ctx.isOriginLocal()) {
               log.trace("Setting executionResponse to " + throwable);
               executionResponse = throwable;
            } else {
               log.trace("Ignoring throwable " + throwable);
               executionResponse = "NONE";
            }
            throw throwable;
         }
      }

      public Object getResponse() throws Exception {
         while (executionResponse == null) {
            Thread.sleep(50);
         }
         return executionResponse;
      }
   }
}
