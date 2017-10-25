package org.infinispan.lock;

import static java.util.concurrent.ForkJoinPool.commonPool;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.ExtendedLockPromise;
import org.infinispan.util.concurrent.locks.LockPromise;
import org.infinispan.util.concurrent.locks.LockState;
import org.infinispan.util.concurrent.locks.impl.InfinispanLock;
import org.testng.annotations.Test;

/**
 * Unit tests for the {@link InfinispanLock}.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
@Test(groups = "unit", testName = "lock.InfinispanLockTest")
public class InfinispanLockTest extends AbstractInfinispanTest {

   public void testTimeout() throws InterruptedException {
      final String lockOwner1 = "LO1";
      final String lockOwner2 = "LO2";

      final InfinispanLock lock = new InfinispanLock(commonPool(), AbstractCacheTest.TIME_SERVICE);
      final LockPromise lockPromise1 = lock.acquire(lockOwner1, 0, TimeUnit.MILLISECONDS);
      final LockPromise lockPromise2 = lock.acquire(lockOwner2, 0, TimeUnit.MILLISECONDS);

      assertTrue(lockPromise1.isAvailable());
      assertTrue(lockPromise2.isAvailable());

      lockPromise1.lock();
      assertEquals(lockOwner1, lock.getLockOwner());
      try {
         lockPromise2.lock();
         fail();
      } catch (TimeoutException e) {
         //expected!
      }
      lock.release(lockOwner1);
      assertNull(lock.getLockOwner());
      assertFalse(lock.isLocked());

      //no side effects
      lock.release(lockOwner2);
      assertFalse(lock.isLocked());
      assertNull(lock.getLockOwner());
   }

   public void testTimeout2() throws InterruptedException {
      final String lockOwner1 = "LO1";
      final String lockOwner2 = "LO2";
      final String lockOwner3 = "LO3";

      final InfinispanLock lock = new InfinispanLock(commonPool(), AbstractCacheTest.TIME_SERVICE);
      final LockPromise lockPromise1 = lock.acquire(lockOwner1, 0, TimeUnit.MILLISECONDS);
      final LockPromise lockPromise2 = lock.acquire(lockOwner2, 0, TimeUnit.MILLISECONDS);
      final LockPromise lockPromise3 = lock.acquire(lockOwner3, 1, TimeUnit.DAYS);

      assertTrue(lockPromise1.isAvailable());
      assertTrue(lockPromise2.isAvailable());
      assertFalse(lockPromise3.isAvailable());

      lockPromise1.lock();
      assertEquals(lockOwner1, lock.getLockOwner());
      try {
         lockPromise2.lock();
         fail();
      } catch (TimeoutException e) {
         //expected!
      }
      lock.release(lockOwner1);
      assertTrue(lock.isLocked());

      assertTrue(lockPromise3.isAvailable());
      lockPromise3.lock();
      assertEquals(lockOwner3, lock.getLockOwner());
      lock.release(lockOwner3);
      assertFalse(lock.isLocked());

      //no side effects
      lock.release(lockOwner2);
      assertFalse(lock.isLocked());
      assertNull(lock.getLockOwner());
   }

   public void testTimeout3() throws InterruptedException {
      final String lockOwner1 = "LO1";
      final String lockOwner2 = "LO2";
      final String lockOwner3 = "LO3";

      final InfinispanLock lock = new InfinispanLock(commonPool(), AbstractCacheTest.TIME_SERVICE);
      final LockPromise lockPromise1 = lock.acquire(lockOwner1, 0, TimeUnit.MILLISECONDS);
      final LockPromise lockPromise2 = lock.acquire(lockOwner2, 1, TimeUnit.DAYS);
      final LockPromise lockPromise3 = lock.acquire(lockOwner3, 1, TimeUnit.DAYS);

      assertTrue(lockPromise1.isAvailable());
      assertFalse(lockPromise2.isAvailable());
      assertFalse(lockPromise3.isAvailable());

      lockPromise1.lock();
      assertEquals(lockOwner1, lock.getLockOwner());

      //premature release. the lock is never acquired by owner 2
      //when the owner 1 releases, owner 3 is able to acquire it
      lock.release(lockOwner2);
      assertTrue(lock.isLocked());
      assertEquals(lockOwner1, lock.getLockOwner());

      lock.release(lockOwner1);
      assertTrue(lock.isLocked());

      assertTrue(lockPromise3.isAvailable());
      lockPromise3.lock();
      assertEquals(lockOwner3, lock.getLockOwner());
      lock.release(lockOwner3);
      assertFalse(lock.isLocked());

      //no side effects
      lock.release(lockOwner2);
      assertFalse(lock.isLocked());
      assertNull(lock.getLockOwner());
   }

   public void testCancel() throws InterruptedException {
      final InfinispanLock lock = new InfinispanLock(commonPool(), AbstractCacheTest.TIME_SERVICE);
      final String lockOwner1 = "LO1";
      final String lockOwner2 = "LO2";
      final String lockOwner3 = "LO3";

      ExtendedLockPromise lockPromise1 = lock.acquire(lockOwner1, 0, TimeUnit.MILLISECONDS); //will be acquired
      ExtendedLockPromise lockPromise2 = lock.acquire(lockOwner2, 0, TimeUnit.MILLISECONDS); //will be timed-out
      ExtendedLockPromise lockPromise3 = lock.acquire(lockOwner3, 1, TimeUnit.DAYS); //will be waiting

      assertTrue(lockPromise1.isAvailable());
      assertTrue(lockPromise2.isAvailable());
      assertFalse(lockPromise3.isAvailable());

      assertEquals(lockOwner1, lock.getLockOwner());

      lockPromise1.cancel(LockState.TIMED_OUT); //no-op
      lockPromise1.lock();

      try {
         lockPromise2.lock();
         fail("TimeoutException expected");
      } catch (TimeoutException e) {
         //expected
      }

      assertEquals(lockOwner1, lock.getLockOwner());

      assertTrue(lockPromise2.isAvailable());
      assertFalse(lockPromise3.isAvailable());

      lockPromise2.cancel(LockState.DEADLOCKED);

      try {
         //check state didn't change
         lockPromise2.lock();
         fail("TimeoutException expected");
      } catch (TimeoutException e) {
         //expected
      }

      assertEquals(lockOwner1, lock.getLockOwner());

      assertFalse(lockPromise3.isAvailable());

      lockPromise3.cancel(LockState.DEADLOCKED);

      try {
         lockPromise3.lock();
         fail("DeadlockDetectedException expected");
      } catch (DeadlockDetectedException e) {
         //expected
      }

      lock.release(lockOwner1);

      assertNull(lock.getLockOwner());
      assertFalse(lock.isLocked());

      lockPromise1 = lock.acquire(lockOwner1, 0, TimeUnit.MILLISECONDS);
      lockPromise2 = lock.acquire(lockOwner2, 1, TimeUnit.DAYS);

      lockPromise1.lock();
      lockPromise1.cancel(LockState.TIMED_OUT);

      lockPromise1.lock(); //should not throw anything

      //lock2 is in WAITING state
      lockPromise2.cancel(LockState.TIMED_OUT);
      assertTrue(lockPromise2.isAvailable());

      lock.release(lockOwner1);
      try {
         lockPromise2.lock();
         fail("TimeoutException expected");
      } catch (TimeoutException e) {
         //expected
      }

      assertNull(lock.getLockOwner());
      assertFalse(lock.isLocked());
   }

   public void testSingleCounter() throws ExecutionException, InterruptedException {
      final NotThreadSafeCounter counter = new NotThreadSafeCounter();
      final InfinispanLock counterLock = new InfinispanLock(commonPool(), AbstractCacheTest.TIME_SERVICE);
      final int numThreads = 8;
      final int maxCounterValue = 100;
      final CyclicBarrier barrier = new CyclicBarrier(numThreads);
      List<Future<Collection<Integer>>> callableResults = new ArrayList<>(numThreads);

      for (int i = 0; i < numThreads; ++i) {
         callableResults.add(fork(() -> {
            final Thread lockOwner = Thread.currentThread();
            assertEquals(0, counter.getCount());
            List<Integer> seenValues = new LinkedList<>();
            barrier.await();
            while (true) {
               counterLock.acquire(lockOwner, 1, TimeUnit.DAYS).lock();
               assertEquals(lockOwner, counterLock.getLockOwner());
               try {
                  int value = counter.getCount();
                  if (value == maxCounterValue) {
                     return seenValues;
                  }
                  seenValues.add(value);
                  counter.setCount(value + 1);
               } finally {
                  counterLock.release(lockOwner);
               }
            }
         }));
      }

      Set<Integer> seenResults = new HashSet<>();
      for (Future<Collection<Integer>> future : callableResults) {
         for (Integer integer : future.get()) {
            assertTrue(seenResults.add(integer));
         }
      }
      assertEquals(maxCounterValue, seenResults.size());
      for (int i = 0; i < maxCounterValue; ++i) {
         assertTrue(seenResults.contains(i));
      }

      assertFalse(counterLock.isLocked());
   }

   private static class NotThreadSafeCounter {
      private int count;

      public int getCount() {
         return count;
      }

      public void setCount(int count) {
         this.count = count;
      }
   }

}
