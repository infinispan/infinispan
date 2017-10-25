package org.infinispan.lock;

import static java.util.concurrent.ForkJoinPool.commonPool;

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
import org.infinispan.util.concurrent.locks.LockPromise;
import org.infinispan.util.concurrent.locks.impl.LockContainer;
import org.infinispan.util.concurrent.locks.impl.PerKeyLockContainer;
import org.infinispan.util.concurrent.locks.impl.StripedLockContainer;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Unit test for {@link LockContainer}.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
@Test(groups = "unit", testName = "lock.LockContainerTest")
public class LockContainerTest extends AbstractInfinispanTest {

   public void testSingleLockWithPerEntry() throws InterruptedException {
      PerKeyLockContainer lockContainer = new PerKeyLockContainer();
      lockContainer.inject(commonPool(), AbstractCacheTest.TIME_SERVICE);
      doSingleLockTest(lockContainer, -1);
   }

   public void testSingleCounterTestPerEntry() throws ExecutionException, InterruptedException {
      PerKeyLockContainer lockContainer = new PerKeyLockContainer();
      lockContainer.inject(commonPool(), AbstractCacheTest.TIME_SERVICE);
      doSingleCounterTest(lockContainer, -1);
   }

   public void testSingleLockWithStriped() throws InterruptedException {
      StripedLockContainer lockContainer = new StripedLockContainer(16);
      lockContainer.inject(commonPool(), AbstractCacheTest.TIME_SERVICE);
      doSingleLockTest(lockContainer, 16);
   }

   public void testSingleCounterWithStriped() throws ExecutionException, InterruptedException {
      StripedLockContainer lockContainer = new StripedLockContainer(16);
      lockContainer.inject(commonPool(), AbstractCacheTest.TIME_SERVICE);
      doSingleCounterTest(lockContainer, 16);
   }

   private void doSingleCounterTest(LockContainer lockContainer, int poolSize) throws InterruptedException, ExecutionException {
      final NotThreadSafeCounter counter = new NotThreadSafeCounter();
      final String key = "key";
      final int numThreads = 8;
      final int maxCounterValue = 100;
      final CyclicBarrier barrier = new CyclicBarrier(numThreads);
      List<Future<Collection<Integer>>> callableResults = new ArrayList<>(numThreads);

      for (int i = 0; i < numThreads; ++i) {
         callableResults.add(fork(() -> {
            final Thread lockOwner = Thread.currentThread();
            AssertJUnit.assertEquals(0, counter.getCount());
            List<Integer> seenValues = new LinkedList<>();
            barrier.await();
            while (true) {
               lockContainer.acquire(key, lockOwner, 1, TimeUnit.DAYS).lock();
               AssertJUnit.assertEquals(lockOwner, lockContainer.getLock(key).getLockOwner());
               try {
                  int value = counter.getCount();
                  if (value == maxCounterValue) {
                     return seenValues;
                  }
                  seenValues.add(value);
                  counter.setCount(value + 1);
               } finally {
                  lockContainer.release(key, lockOwner);
               }
            }
         }));
      }

      Set<Integer> seenResults = new HashSet<>();
      for (Future<Collection<Integer>> future : callableResults) {
         for (Integer integer : future.get()) {
            AssertJUnit.assertTrue(seenResults.add(integer));
         }
      }
      AssertJUnit.assertEquals(maxCounterValue, seenResults.size());
      for (int i = 0; i < maxCounterValue; ++i) {
         AssertJUnit.assertTrue(seenResults.contains(i));
      }

      AssertJUnit.assertEquals(0, lockContainer.getNumLocksHeld());
      if (poolSize == -1) {
         AssertJUnit.assertEquals(0, lockContainer.size());
      } else {
         AssertJUnit.assertEquals(poolSize, lockContainer.size());
      }
   }

   private void doSingleLockTest(LockContainer container, int poolSize) throws InterruptedException {
      final String lockOwner1 = "LO1";
      final String lockOwner2 = "LO2";
      final String lockOwner3 = "LO3";

      final LockPromise lockPromise1 = container.acquire("key", lockOwner1, 0, TimeUnit.MILLISECONDS);
      final LockPromise lockPromise2 = container.acquire("key", lockOwner2, 0, TimeUnit.MILLISECONDS);
      final LockPromise lockPromise3 = container.acquire("key", lockOwner3, 0, TimeUnit.MILLISECONDS);

      AssertJUnit.assertEquals(1, container.getNumLocksHeld());
      if (poolSize == -1) {
         //dynamic
         AssertJUnit.assertEquals(1, container.size());
      } else {
         AssertJUnit.assertEquals(poolSize, container.size());
      }

      acquireLock(lockPromise1, false);
      acquireLock(lockPromise2, true);
      acquireLock(lockPromise3, true);

      AssertJUnit.assertEquals(1, container.getNumLocksHeld());
      if (poolSize == -1) {
         //dynamic
         AssertJUnit.assertEquals(1, container.size());
      } else {
         AssertJUnit.assertEquals(poolSize, container.size());
      }

      container.release("key", lockOwner2);
      container.release("key", lockOwner3);

      AssertJUnit.assertEquals(1, container.getNumLocksHeld());
      if (poolSize == -1) {
         //dynamic
         AssertJUnit.assertEquals(1, container.size());
      } else {
         AssertJUnit.assertEquals(poolSize, container.size());
      }

      container.release("key", lockOwner1);

      AssertJUnit.assertEquals(0, container.getNumLocksHeld());
      if (poolSize == -1) {
         //dynamic
         AssertJUnit.assertEquals(0, container.size());
      } else {
         AssertJUnit.assertEquals(poolSize, container.size());
      }
   }

   private void acquireLock(LockPromise promise, boolean timeout) throws InterruptedException {
      try {
         promise.lock();
         AssertJUnit.assertFalse(timeout);
      } catch (TimeoutException e) {
         AssertJUnit.assertTrue(timeout);
      }
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
