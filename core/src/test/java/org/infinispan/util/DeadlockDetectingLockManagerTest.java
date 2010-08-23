package org.infinispan.util;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.verify;

import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.xa.GlobalTransactionFactory;
import org.infinispan.transaction.xa.DeadlockDetectingGlobalTransaction;
import org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager;
import org.infinispan.util.concurrent.locks.containers.LockContainer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Tests functionality in {@link org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "unit", testName = "util.DeadlockDetectingLockManagerTest")
public class DeadlockDetectingLockManagerTest extends AbstractInfinispanTest {

   DeadlockDetectingLockManagerMock lockManager;
   GlobalTransactionFactory gtf = new GlobalTransactionFactory(true);
   Configuration config = new Configuration();
   private LockContainer lc;
   private static final int SPIN_DURATION = 1000;
   private DeadlockDetectingGlobalTransaction lockOwner;

   @BeforeMethod
   public void setUp() {
      lc = createMock(LockContainer.class);
      lockManager = new DeadlockDetectingLockManagerMock(SPIN_DURATION, true, lc, config);
      lockOwner = (DeadlockDetectingGlobalTransaction) gtf.instantiateGlobalTransaction();
   }


   public void testNoTransaction() throws Exception {
      InvocationContext nonTx = new NonTxInvocationContext();

//      expect(lc.acquireLock("k",config.getLockAcquisitionTimeout(), TimeUnit.MILLISECONDS)).andReturn(EasyMock.<Lock>anyObject());
      Lock mockLock = createNiceMock(Lock.class);
      expect(lc.acquireLock("k",config.getLockAcquisitionTimeout(), TimeUnit.MILLISECONDS)).andReturn(mockLock);      
      expect(lc.acquireLock("k",config.getLockAcquisitionTimeout(), TimeUnit.MILLISECONDS)).andReturn(null);
      replay(lc);
      assert lockManager.lockAndRecord("k",nonTx);
      assert !lockManager.lockAndRecord("k",nonTx);
      verify();
   }

   public void testLockHeldByThread() throws Exception {
      InvocationContext localTxContext = new LocalTxInvocationContext();

      //this makes sure that we cannot acquire lock from the first try
      expect(lc.acquireLock("k", SPIN_DURATION, TimeUnit.MILLISECONDS)).andReturn(null);
      lockManager.setOwner(Thread.currentThread() );
      //next lock acquisition will succeed
      Lock mockLock = createNiceMock(Lock.class);
      expect(lc.acquireLock("k", SPIN_DURATION, TimeUnit.MILLISECONDS)).andReturn(mockLock);
      replay(lc);

      assert lockManager.lockAndRecord("k", localTxContext);
      assert lockManager.getOverlapWithNotDeadlockAwareLockOwners() == 1;
   }

   public void testLocalDeadlock() throws Exception {
      final DeadlockDetectingGlobalTransaction ddgt = (DeadlockDetectingGlobalTransaction) gtf.instantiateGlobalTransaction();

      InvocationContext localTxContext = new LocalTxInvocationContext() {
         @Override
         public Object getLockOwner() {
            return ddgt;
         }
      };

      ddgt.setCoinToss(0);
      lockOwner.setCoinToss(-1);
      assert ddgt.thisWillInterrupt(lockOwner);

      //this makes sure that we cannot acquire lock from the first try
      expect(lc.acquireLock("k", SPIN_DURATION, TimeUnit.MILLISECONDS)).andReturn(null);
      Lock mockLock = createNiceMock(Lock.class);
      expect(lc.acquireLock("k", SPIN_DURATION, TimeUnit.MILLISECONDS)).andReturn(mockLock);
      lockOwner.setRemote(false);
      lockManager.setOwner(lockOwner);
      lockManager.setOwnsLock(true);
      replay(lc);
      assert lockManager.lockAndRecord("k", localTxContext);
      assert lockManager.getDetectedLocalDeadlocks() == 1;
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
