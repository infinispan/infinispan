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

import org.infinispan.test.PerCacheExecutorThread;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager;
import org.infinispan.util.concurrent.locks.LockManager;

import java.util.concurrent.CountDownLatch;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public abstract class BaseDldOptimisticLockingTest extends BaseDldTest {

   protected void testSymmetricDeadlock(Object k0, Object k1) {

      CountDownLatch replLatch = new CountDownLatch(1);
      rpcManager0.setReplicationLatch(replLatch);
      rpcManager1.setReplicationLatch(replLatch);

      DeadlockDetectingLockManager ddLm0 = (DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(0));
      ddLm0.setExposeJmxStats(true);
      DeadlockDetectingLockManager ddLm1 = (DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(1));
      ddLm1.setExposeJmxStats(true);


      PerCacheExecutorThread t0 = new PerCacheExecutorThread(cache(0), 0);
      PerCacheExecutorThread t1 = new PerCacheExecutorThread(cache(1), 1);

      assertEquals(PerCacheExecutorThread.OperationsResult.BEGGIN_TX_OK, t0.execute(PerCacheExecutorThread.Operations.BEGGIN_TX));
      assertEquals(PerCacheExecutorThread.OperationsResult.BEGGIN_TX_OK, t1.execute(PerCacheExecutorThread.Operations.BEGGIN_TX));

      t0.setKeyValue(k0, "k0_0");
      t1.setKeyValue(k1, "k1_1");

      t0.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);
      t1.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);
      t0.execute(PerCacheExecutorThread.Operations.FORCE2PC);
      t1.execute(PerCacheExecutorThread.Operations.FORCE2PC);

      t0.setKeyValue(k1, "k1_0");
      t1.setKeyValue(k0, "k0_1");

      assertEquals(t0.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE), PerCacheExecutorThread.OperationsResult.PUT_KEY_VALUE_OK);
      assertEquals(t1.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE), PerCacheExecutorThread.OperationsResult.PUT_KEY_VALUE_OK);

      log.info("---Before commit");
      t0.executeNoResponse(PerCacheExecutorThread.Operations.COMMIT_TX);
      t1.executeNoResponse(PerCacheExecutorThread.Operations.COMMIT_TX);

      replLatch.countDown();


      Object t0Response = t0.waitForResponse();
      Object t1Response = t1.waitForResponse();

      assert xor(t0Response instanceof Exception, t1Response instanceof Exception);

      if (t0Response instanceof Exception) {
         Object o = cache(0).get(k0);
         assert o != null;
         assert o.equals("k0_1");
      } else {
         Object o = cache(1).get(k0);
         assert o != null;
         assert o.equals("k0_0");
      }

      assert ddLm0.getDetectedRemoteDeadlocks() + ddLm1.getDetectedRemoteDeadlocks() >= 1;

      LockManager lm0 = TestingUtil.extractComponent(cache(0), LockManager.class);
      assert !lm0.isLocked("key") : "It is locked by " + lm0.getOwner("key");
      LockManager lm1 = TestingUtil.extractComponent(cache(1), LockManager.class);
      assert !lm1.isLocked("key") : "It is locked by " + lm1.getOwner("key");
   }
}
