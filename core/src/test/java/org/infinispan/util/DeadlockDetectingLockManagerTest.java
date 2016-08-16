package org.infinispan.util;

import static org.testng.Assert.fail;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager;
import org.infinispan.util.concurrent.locks.LockPromise;
import org.infinispan.util.concurrent.locks.impl.LockContainer;
import org.infinispan.util.concurrent.locks.impl.PerKeyLockContainer;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests functionality in {@link org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "unit", testName = "util.DeadlockDetectingLockManagerTest")
public class DeadlockDetectingLockManagerTest extends AbstractInfinispanTest {

   private DeadlockDetectingLockManagerMock lockManager;

   @BeforeMethod
   public void setUp() {
      lockManager = new DeadlockDetectingLockManagerMock(createLockContainer());
   }

   public void testNoTransaction() throws Exception {
      lockManager.lock("k", "aThread", 0, TimeUnit.MILLISECONDS).lock();
      try {
         lockManager.lock("k", "anotherThread", 0, TimeUnit.MILLISECONDS).lock();
         fail("TimeoutException expected!");
      } catch (TimeoutException e) {
         //expected
      }
   }

   public void testLockHeldByThread() throws Exception {
      lockManager.lock("k", "aThread", 0, TimeUnit.MILLISECONDS).lock();
      try {
         LockPromise promise = lockManager.lock("k", new DldGlobalTransaction(), 1000, TimeUnit.MILLISECONDS);
         lockManager.run(); //runs the deadlock check
         promise.lock();
         fail("TimeoutException expected!");
      } catch (TimeoutException e) {
         //expected
      }

      AssertJUnit.assertEquals(1, lockManager.getOverlapWithNotDeadlockAwareLockOwners());
   }

   public void testLocalDeadlock() throws Exception {
      final DldGlobalTransaction ddgt = new DldGlobalTransaction();
      final DldGlobalTransaction lockOwner = new DldGlobalTransaction();

      ddgt.setCoinToss(0);
      lockOwner.setCoinToss(1);
      lockOwner.setRemote(false);
      lockOwner.setLockIntention(Collections.singleton("k"));
      AssertJUnit.assertTrue(ddgt.wouldLose(lockOwner));

      lockManager.setOwner(lockOwner);
      lockManager.setOwnsLock(true);

      lockManager.lock("k", lockOwner, 0, TimeUnit.MILLISECONDS).lock();

      try {
         LockPromise lockPromise = lockManager.lock("k", ddgt, 1500, TimeUnit.MILLISECONDS);
         lockManager.run(); //runs the deadlock checker
         lockPromise.lock();
         assert false;
      } catch (DeadlockDetectedException e) {
         //expected
      }
      AssertJUnit.assertEquals(1, lockManager.getDetectedLocalDeadlocks());
   }

   private LockContainer createLockContainer() {
      PerKeyLockContainer lockContainer = new PerKeyLockContainer(32, AnyEquivalence.getInstance());
      lockContainer.inject(TIME_SERVICE);
      return lockContainer;
   }

   public static class DeadlockDetectingLockManagerMock extends DeadlockDetectingLockManager {

      private Object owner;
      private boolean ownsLock;

      public DeadlockDetectingLockManagerMock(LockContainer lockContainer) {
         super.lockContainer = lockContainer;
         this.exposeJmxStats = true;
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
