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

import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.PerCacheExecutorThread;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;

/**
 * Tests deadlock detection functionality for local caches.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "tx.dld.LocalDeadlockDetectionTest")
public class LocalDeadlockDetectionTest extends SingleCacheManagerTest {

   private PerCacheExecutorThread t1;
   private PerCacheExecutorThread t2;
   private DeadlockDetectingLockManager lockManager;
   private Object response1;
   private Object response2;

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createLocalCacheManager(false);
      Configuration configuration = createConfig();
      configuration.fluent().transaction().lockingMode(LockingMode.PESSIMISTIC);
      cacheManager.defineConfiguration("test", configuration);
      cache = cacheManager.getCache("test");
      lockManager = (DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache);
      return cacheManager;
   }

   protected Configuration createConfig() {
      Configuration configuration = getDefaultStandaloneConfig(true);
      configuration.setEnableDeadlockDetection(true);
      configuration.setUseLockStriping(false);
      configuration.setExposeJmxStatistics(true);
      return configuration;
   }

   @BeforeMethod
   public void startExecutors() {
      t1 = new PerCacheExecutorThread(cache, 0);
      t2 = new PerCacheExecutorThread(cache, 1);
      lockManager.resetStatistics();
   }


   @AfterMethod
   public void stopExecutors() {
      t1.stopThread();
      t2.stopThread();
   }

   public void testDldPutAndPut() {
      testLocalVsLocalTxDeadlock(PerCacheExecutorThread.Operations.PUT_KEY_VALUE,
                                 PerCacheExecutorThread.Operations.PUT_KEY_VALUE);
      if (response1 instanceof Exception) {
         assertEquals("value_1_t2", cache.get("k1"));
         assertEquals("value_2_t2", cache.get("k2"));
      } else {
         assertEquals("value_1_t1", cache.get("k1"));
         assertEquals("value_2_t1", cache.get("k2"));
      }
   }

   public void testDldPutAndRemove() {
      testLocalVsLocalTxDeadlock(PerCacheExecutorThread.Operations.PUT_KEY_VALUE,
                                 PerCacheExecutorThread.Operations.REMOVE_KEY);
      if (response1 instanceof Exception) {
         assertEquals(cache.get("k1"), null);
         assertEquals("value_2_t2", cache.get("k2"));
      } else {
         assertEquals("value_1_t1", cache.get("k1"));
         assertEquals(null, cache.get("k2"));
      }
   }

   public void testDldRemoveAndPut() {
      testLocalVsLocalTxDeadlock(PerCacheExecutorThread.Operations.REMOVE_KEY,
                                 PerCacheExecutorThread.Operations.PUT_KEY_VALUE);
      if (response1 instanceof Exception) {
         log.trace("t1 failure");
         assertEquals(cache.get("k1"), "value_1_t2");
         assertEquals(cache.get("k2"), null);
      } else {
         log.trace("t2 failure");
         assertEquals(cache.get("k1"), null);
         assertEquals(cache.get("k2"), "value_2_t1");
      }
   }

   public void testDldRemoveAndRemove() {
      testLocalVsLocalTxDeadlock(PerCacheExecutorThread.Operations.REMOVE_KEY,
                                 PerCacheExecutorThread.Operations.REMOVE_KEY);
      if (response1 instanceof Exception) {
         log.tracef("t1 failure");
         assertEquals(cache.get("k1"), null);
         assertEquals(cache.get("k2"), null);
      } else {
         log.tracef("t2 failure");
         assertEquals(cache.get("k1"), null);
         assertEquals(cache.get("k2"), null);
      }
   }

   public void testDldPutAndReplace() {

      cache.put("k1", "initial_1");
      cache.put("k2", "initial_2");

      testLocalVsLocalTxDeadlock(PerCacheExecutorThread.Operations.PUT_KEY_VALUE,
                                 PerCacheExecutorThread.Operations.REPLACE_KEY_VALUE);
      if (response1 instanceof Exception) {
         log.tracef("t1 failure");
         assertEquals(cache.get("k1"), "value_1_t2");
         assertEquals(cache.get("k2"), "value_2_t2");
      } else {
         log.tracef("t2 failure");
         assertEquals(cache.get("k1"), "value_1_t1");
         assertEquals(cache.get("k2"), "value_2_t1");
      }
   }

   public void testDldReplaceAndPut() {

      cache.put("k1", "initial_1");
      cache.put("k2", "initial_2");

      testLocalVsLocalTxDeadlock(PerCacheExecutorThread.Operations.REPLACE_KEY_VALUE,
                                 PerCacheExecutorThread.Operations.PUT_KEY_VALUE);
      if (response1 instanceof Exception) {
         log.tracef("t1 failure");
         assertEquals(cache.get("k1"), "value_1_t2");
         assertEquals(cache.get("k2"), "value_2_t2");
      } else {
         log.tracef("t2 failure");
         assertEquals(cache.get("k1"), "value_1_t1");
         assertEquals(cache.get("k2"), "value_2_t1");
      }
   }


   private void testLocalVsLocalTxDeadlock(PerCacheExecutorThread.Operations firstOperation, PerCacheExecutorThread.Operations secondOperation) {

      assert PerCacheExecutorThread.OperationsResult.BEGIN_TX_OK == t1.execute(PerCacheExecutorThread.Operations.BEGIN_TX);
      assert PerCacheExecutorThread.OperationsResult.BEGIN_TX_OK == t2.execute(PerCacheExecutorThread.Operations.BEGIN_TX);

      t1.setKeyValue("k1", "value_1_t1");
      t2.setKeyValue("k2", "value_2_t2");

      assertEquals(t1.execute(firstOperation), firstOperation.getCorrespondingOkResult());
      assertEquals(t2.execute(firstOperation), firstOperation.getCorrespondingOkResult());

      assert lockManager.isLocked("k1");
      assert lockManager.isLocked("k2");


      t1.setKeyValue("k2", "value_2_t1");
      t2.setKeyValue("k1", "value_1_t2");
      t1.executeNoResponse(secondOperation);
      t2.executeNoResponse(secondOperation);

      response1 = t1.waitForResponse();
      response2 = t2.waitForResponse();

      assert xor(response1 instanceof DeadlockDetectedException, response2 instanceof DeadlockDetectedException) : "expected one and only one exception: " + response1 + ", " + response2;
      assert xor(response1 == secondOperation.getCorrespondingOkResult(), response2 == secondOperation.getCorrespondingOkResult()) : "expected one and only one exception: " + response1 + ", " + response2;

      assert lockManager.isLocked("k1");
      assert lockManager.isLocked("k2");
      assert lockManager.getOwner("k1") == lockManager.getOwner("k2");

      if (response1 instanceof Exception) {
         assertEquals(PerCacheExecutorThread.OperationsResult.COMMIT_TX_OK, t2.execute(PerCacheExecutorThread.Operations.COMMIT_TX));
         assert t1.execute(PerCacheExecutorThread.Operations.COMMIT_TX) instanceof RollbackException;
      } else {
         assert PerCacheExecutorThread.OperationsResult.COMMIT_TX_OK == t1.execute(PerCacheExecutorThread.Operations.COMMIT_TX);
         assert t2.execute(PerCacheExecutorThread.Operations.COMMIT_TX) instanceof RollbackException;
      }
      assert lockManager.getNumberOfLocksHeld() == 0;
      assertEquals(lockManager.getDetectedLocalDeadlocks(), 1);
   }

}
