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

import org.infinispan.api.mvcc.LockAssert;
import org.infinispan.config.Configuration;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.interceptors.DeadlockDetectingInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.PerCacheExecutorThread;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

/**
 * Functional test for deadlock detection.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(testName = "tx.dld.DldLazyLockingReplicationTest", groups = "functional")
public class DldOptimisticLockingReplicationTest extends BaseDldOptimisticLockingTest {

   protected CountDownLatch replicationLatch;
   protected PerCacheExecutorThread t1;
   protected PerCacheExecutorThread t2;
   protected DeadlockDetectingLockManager ddLm1;
   protected DeadlockDetectingLockManager ddLm2; 

   protected void createCacheManagers() throws Throwable {
      Configuration config = createConfiguration();
      assert config.isEnableDeadlockDetection();
      createClusteredCaches(2, config);
      assert config.isEnableDeadlockDetection();

      assert cache(0).getConfiguration().isEnableDeadlockDetection();
      assert cache(1).getConfiguration().isEnableDeadlockDetection();
      assert !cache(0).getConfiguration().isExposeJmxStatistics();
      assert !cache(1).getConfiguration().isExposeJmxStatistics();

      ((DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(0))).setExposeJmxStats(true);
      ((DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(1))).setExposeJmxStats(true);

      rpcManager0 = replaceRpcManager(cache(0));
      rpcManager1 = replaceRpcManager(cache(1));

      assert TestingUtil.extractComponent(cache(0), RpcManager.class) instanceof ControlledRpcManager;
      assert TestingUtil.extractComponent(cache(1), RpcManager.class) instanceof ControlledRpcManager;

      ddLm1 = (DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(0));
      ddLm2 = (DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(1));
   }

   protected Configuration createConfiguration() {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC, true);
      config.setEnableDeadlockDetection(true);
      config.setSyncCommitPhase(true);
      config.setSyncRollbackPhase(true);
      config.setUseLockStriping(false);
      return config;
   }


   @BeforeMethod
   public void beforeMethod() {
      t1 = new PerCacheExecutorThread(cache(0), 1);
      t2 = new PerCacheExecutorThread(cache(1), 2);
      replicationLatch = new CountDownLatch(1);
      rpcManager0.setReplicationLatch(replicationLatch);
      rpcManager1.setReplicationLatch(replicationLatch);
      log.trace("_________________________ Here it begins");
   }

   @AfterMethod
   public void afterMethod() {
      t1.stopThread();
      t2.stopThread();
      ((DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(0))).resetStatistics();
      ((DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(1))).resetStatistics();
   }

   public void testSymmetricDeadlock() {
      super.testSymmetricDeadlock("k0", "k1");
   }

   public void testExpectedInnerStructure() {
      LockManager lockManager = TestingUtil.extractComponent(cache(0), LockManager.class);
      assert lockManager instanceof DeadlockDetectingLockManager;

      InterceptorChain ic = TestingUtil.extractComponent(cache(0), InterceptorChain.class);
      assert ic.containsInterceptorType(DeadlockDetectingInterceptor.class);
   }

   public void testDeadlockDetectedOneTx() throws Exception {
      t1.setKeyValue("key", "value1");

      LockManager lm2 = TestingUtil.extractComponent(cache(1), LockManager.class);
      NonTxInvocationContext ctx = cache(1).getAdvancedCache().getInvocationContextContainer().createNonTxInvocationContext();
      lm2.lockAndRecord("key", ctx);
      assert lm2.isLocked("key");


      assert PerCacheExecutorThread.OperationsResult.BEGGIN_TX_OK == t1.execute(PerCacheExecutorThread.Operations.BEGGIN_TX) : "but received " + t1.lastResponse();
      t1.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);

      t1.clearResponse();
      t1.executeNoResponse(PerCacheExecutorThread.Operations.COMMIT_TX);

      replicationLatch.countDown();
      System.out.println("Now replication is triggered");

      t1.waitForResponse();


      Object t1CommitRsp = t1.lastResponse();

      assert t1CommitRsp instanceof Exception : "expected exception, received " + t1.lastResponse();

      LockManager lm1 = TestingUtil.extractComponent(cache(0), LockManager.class);
      assert !lm1.isLocked("key") : "It is locked by " + lm1.getOwner("key");

      lm2.unlock("key");
      assert !lm2.isLocked("key");
      assert !lm1.isLocked("key");
   }

   public void testLockReleasedWhileTryingToAcquire() throws Exception {
      t1.setKeyValue("key", "value1");

      LockManager lm2 = TestingUtil.extractComponent(cache(1), LockManager.class);
      NonTxInvocationContext ctx = cache(1).getAdvancedCache().getInvocationContextContainer().createNonTxInvocationContext();
      lm2.lockAndRecord("key", ctx);
      assert lm2.isLocked("key");


      assert PerCacheExecutorThread.OperationsResult.BEGGIN_TX_OK == t1.execute(PerCacheExecutorThread.Operations.BEGGIN_TX) : "but received " + t1.lastResponse();
      t1.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);

      t1.clearResponse();
      t1.executeNoResponse(PerCacheExecutorThread.Operations.COMMIT_TX);

      replicationLatch.countDown();

      Thread.sleep(3000); //just to make sure the remote tx thread managed to spin around for some times.
      lm2.unlock("key");

      t1.waitForResponse();


      Object t1CommitRsp = t1.lastResponse();

      assert t1CommitRsp == PerCacheExecutorThread.OperationsResult.COMMIT_TX_OK : "expected true, received " + t1.lastResponse();

      LockManager lm1 = TestingUtil.extractComponent(cache(0), LockManager.class);
      assert !lm1.isLocked("key") : "It is locked by " + lm1.getOwner("key");

      assert !lm2.isLocked("key");
      assert !lm1.isLocked("key");
   }

}
