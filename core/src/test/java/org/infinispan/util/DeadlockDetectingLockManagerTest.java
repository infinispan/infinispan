package org.infinispan.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.LocalTransaction;
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
      lockManager.injectTimeService(TIME_SERVICE);
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
      return new LocalTxInvocationContext(mock(LocalTransaction.class)) {
         @Override
         public Object getLockOwner() {
            return ddgt;
         }
      };
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
