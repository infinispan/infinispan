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
package org.infinispan.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager;
import org.infinispan.util.concurrent.locks.containers.LockContainer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests functionality in {@link org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "unit", testName = "util.DeadlockDetectingLockManagerTest")
public class DeadlockDetectingLockManagerTest extends AbstractInfinispanTest {

   DeadlockDetectingLockManagerMock lockManager;
   Configuration config = new ConfigurationBuilder().build();
   private LockContainer lc;
   private static final int SPIN_DURATION = 1000;
   private DldGlobalTransaction lockOwner;

   @BeforeMethod
   public void setUp() {
      lc = mock(LockContainer.class);
      lockManager = new DeadlockDetectingLockManagerMock(SPIN_DURATION, true, lc, config);
      lockOwner = (DldGlobalTransaction) TransactionFactory.TxFactoryEnum.DLD_NORECOVERY_XA.newGlobalTransaction();
   }


   public void testNoTransaction() throws Exception {
      InvocationContext nonTx = new NonTxInvocationContext(AnyEquivalence.getInstance());

      Lock mockLock = mock(Lock.class);
      when(lc.acquireLock(nonTx.getLockOwner(), "k", config.locking().lockAcquisitionTimeout(), TimeUnit.MILLISECONDS)).thenReturn(mockLock).thenReturn(null);

      assert lockManager.lockAndRecord("k", nonTx, config.locking().lockAcquisitionTimeout());
      assert !lockManager.lockAndRecord("k", nonTx, config.locking().lockAcquisitionTimeout());

   }

   public void testLockHeldByThread() throws Exception {
      InvocationContext localTxContext = buildLocalTxIc(new DldGlobalTransaction());

      Lock mockLock = mock(Lock.class);
      //this makes sure that we cannot acquire lock from the first try
      when(lc.acquireLock(localTxContext.getLockOwner(), "k", SPIN_DURATION, TimeUnit.MILLISECONDS)).thenReturn(null).thenReturn(mockLock);
      lockManager.setOwner(Thread.currentThread() );
      //next lock acquisition will succeed

      assert lockManager.lockAndRecord("k", localTxContext, config.locking().lockAcquisitionTimeout());
      assert lockManager.getOverlapWithNotDeadlockAwareLockOwners() == 1;
   }

   public void testLocalDeadlock() throws Exception {
      final DldGlobalTransaction ddgt = (DldGlobalTransaction) TransactionFactory.TxFactoryEnum.DLD_NORECOVERY_XA.newGlobalTransaction();

      InvocationContext localTxContext = buildLocalTxIc(ddgt);

      ddgt.setCoinToss(0);
      lockOwner.setCoinToss(1);
      assert ddgt.wouldLose(lockOwner);

      //this makes sure that we cannot acquire lock from the first try
      Lock mockLock = mock(Lock.class);
      when(lc.acquireLock(localTxContext.getLockOwner(), "k", SPIN_DURATION, TimeUnit.MILLISECONDS)).thenReturn(null).thenReturn(mockLock);
      lockOwner.setRemote(false);
      lockOwner.setLockIntention("k");
      lockManager.setOwner(lockOwner);
      lockManager.setOwnsLock(true);
      try {
         lockManager.lockAndRecord("k", localTxContext, config.locking().lockAcquisitionTimeout());
         assert false;
      } catch (DeadlockDetectedException e) {
         //expected
      }
      assertEquals(1l,lockManager.getDetectedLocalDeadlocks());
   }

   private InvocationContext buildLocalTxIc(final DldGlobalTransaction ddgt) {
      InvocationContext localTxContext = new LocalTxInvocationContext(AnyEquivalence.getInstance()) {
         @Override
         public Object getLockOwner() {
            return ddgt;
         }
      };
      return localTxContext;
   }

   public static class DeadlockDetectingLockManagerMock extends DeadlockDetectingLockManager {

      private Object owner;
      private boolean ownsLock;

      public DeadlockDetectingLockManagerMock(long spinDuration, boolean exposeJmxStats, LockContainer lockContainer, Configuration configuration) {
         this.spinDuration = spinDuration;
         this.exposeJmxStats = exposeJmxStats;
         super.lockContainer = lockContainer;
         this.configuration = configuration;
      }

      public void setOwner(Object owner) {
         this.owner = owner;
      }

      public void setOwnsLock(boolean ownsLock) {
         this.ownsLock = ownsLock;
      }

      @Override
      public Object getOwner(Object key) {
         return owner;
      }

      @Override
      public boolean ownsLock(Object key, Object owner) {
         return ownsLock;
      }
   }
}
