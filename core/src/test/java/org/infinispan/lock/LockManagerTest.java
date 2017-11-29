package org.infinispan.lock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.LockPromise;
import org.infinispan.util.concurrent.locks.impl.DefaultLockManager;
import org.infinispan.util.concurrent.locks.impl.PerKeyLockContainer;
import org.infinispan.util.concurrent.locks.impl.StripedLockContainer;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link LockManagerTest}.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
@Test(groups = "unit", testName = "lock.LockManagerTest")
public class LockManagerTest extends AbstractInfinispanTest {

   private final ScheduledExecutorService mockExecutor;

   public LockManagerTest() {
      mockExecutor = mock(ScheduledExecutorService.class);
      ScheduledFuture future = mock(ScheduledFuture.class);
      when(future.cancel(anyBoolean())).thenReturn(true);
      //noinspection unchecked
      when(mockExecutor.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenReturn(future);
      //noinspection unchecked
      when(mockExecutor.schedule(any(Callable.class), anyLong(), any(TimeUnit.class))).thenReturn(future);
   }

   public void testSingleCounterPerKey() throws ExecutionException, InterruptedException {
      DefaultLockManager lockManager = new DefaultLockManager();
      PerKeyLockContainer lockContainer = new PerKeyLockContainer();
      lockContainer.inject(AbstractCacheTest.TIME_SERVICE);
      lockManager.inject(lockContainer, null, mockExecutor);
      doSingleCounterTest(lockManager);
   }

   public void testSingleCounterStripped() throws ExecutionException, InterruptedException {
      DefaultLockManager lockManager = new DefaultLockManager();
      StripedLockContainer lockContainer = new StripedLockContainer(16);
      lockContainer.inject(AbstractCacheTest.TIME_SERVICE);
      lockManager.inject(lockContainer, null, mockExecutor);
      doSingleCounterTest(lockManager);
   }

   public void testMultipleCounterPerKey() throws ExecutionException, InterruptedException {
      DefaultLockManager lockManager = new DefaultLockManager();
      PerKeyLockContainer lockContainer = new PerKeyLockContainer();
      lockContainer.inject(AbstractCacheTest.TIME_SERVICE);
      lockManager.inject(lockContainer, null, mockExecutor);
      doMultipleCounterTest(lockManager);
   }

   public void testMultipleCounterStripped() throws ExecutionException, InterruptedException {
      DefaultLockManager lockManager = new DefaultLockManager();
      StripedLockContainer lockContainer = new StripedLockContainer(16);
      lockContainer.inject(AbstractCacheTest.TIME_SERVICE);
      lockManager.inject(lockContainer, null, mockExecutor);
      doMultipleCounterTest(lockManager);
   }

   public void testTimeoutPerKey() throws ExecutionException, InterruptedException {
      DefaultLockManager lockManager = new DefaultLockManager();
      PerKeyLockContainer lockContainer = new PerKeyLockContainer();
      lockContainer.inject(AbstractCacheTest.TIME_SERVICE);
      lockManager.inject(lockContainer, null, mockExecutor);
      doTestWithFailAcquisition(lockManager);
   }

   public void testTimeoutStripped() throws ExecutionException, InterruptedException {
      DefaultLockManager lockManager = new DefaultLockManager();
      StripedLockContainer lockContainer = new StripedLockContainer(16);
      lockContainer.inject(AbstractCacheTest.TIME_SERVICE);
      lockManager.inject(lockContainer, null, mockExecutor);
      doTestWithFailAcquisition(lockManager);
   }

   private void doSingleCounterTest(LockManager lockManager) throws ExecutionException, InterruptedException {
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
               lockManager.lock(key, lockOwner, 1, TimeUnit.MINUTES).lock();
               AssertJUnit.assertEquals(lockOwner, lockManager.getOwner(key));
               AssertJUnit.assertTrue(lockManager.isLocked(key));
               AssertJUnit.assertTrue(lockManager.ownsLock(key, lockOwner));
               try {
                  int value = counter.getCount();
                  if (value == maxCounterValue) {
                     return seenValues;
                  }
                  seenValues.add(value);
                  counter.setCount(value + 1);
               } finally {
                  lockManager.unlock(key, lockOwner);
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

      AssertJUnit.assertEquals(0, lockManager.getNumberOfLocksHeld());
   }

   private void doMultipleCounterTest(LockManager lockManager) throws ExecutionException, InterruptedException {
      final int numCounters = 8;
      final NotThreadSafeCounter[] counters = new NotThreadSafeCounter[numCounters];
      final String[] keys = new String[numCounters];
      final int numThreads = 8;
      final int maxCounterValue = 100;
      final CyclicBarrier barrier = new CyclicBarrier(numThreads);

      for (int i = 0; i < numCounters; ++i) {
         counters[i] = new NotThreadSafeCounter();
         keys[i] = "key-" + i;
      }

      List<Future<Collection<Integer>>> callableResults = new ArrayList<>(numThreads);

      for (int i = 0; i < numThreads; ++i) {
         final List<String> threadKeys = new ArrayList<>(Arrays.asList(keys));
         Collections.shuffle(threadKeys);
         callableResults.add(fork(() -> {
            final Thread lockOwner = Thread.currentThread();
            List<Integer> seenValues = new LinkedList<>();
            barrier.await();
            while (true) {
               lockManager.lockAll(threadKeys, lockOwner, 1, TimeUnit.MINUTES).lock();
               for (String key : threadKeys) {
                  AssertJUnit.assertEquals(lockOwner, lockManager.getOwner(key));
                  AssertJUnit.assertTrue(lockManager.isLocked(key));
                  AssertJUnit.assertTrue(lockManager.ownsLock(key, lockOwner));
               }
               try {
                  int value = -1;
                  for (NotThreadSafeCounter counter : counters) {
                     if (value == -1) {
                        value = counter.getCount();
                        if (value == maxCounterValue) {
                           return seenValues;
                        }
                        seenValues.add(value);
                     } else {
                        AssertJUnit.assertEquals(value, counter.getCount());
                     }
                     counter.setCount(value + 1);
                  }
               } finally {
                  lockManager.unlockAll(threadKeys, lockOwner);
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

      AssertJUnit.assertEquals(0, lockManager.getNumberOfLocksHeld());
   }

   private void doTestWithFailAcquisition(LockManager lockManager) throws InterruptedException {
      final String lockOwner1 = "LO1";
      final String lockOwner2 = "LO2";
      final String key = "key";
      final String key2 = "key2";
      final String key3 = "key2";

      lockManager.lock(key, lockOwner1, 0, TimeUnit.MILLISECONDS).lock();
      AssertJUnit.assertEquals(lockOwner1, lockManager.getOwner(key));
      AssertJUnit.assertTrue(lockManager.isLocked(key));
      AssertJUnit.assertTrue(lockManager.ownsLock(key, lockOwner1));

      try {
         lockManager.lockAll(Arrays.asList(key, key2, key3), lockOwner2, 0, TimeUnit.MILLISECONDS).lock();
         AssertJUnit.assertEquals(1, lockManager.getNumberOfLocksHeld());
         AssertJUnit.fail();
      } catch (TimeoutException t) {
         //expected
      }

      AssertJUnit.assertEquals(lockOwner1, lockManager.getOwner(key));
      AssertJUnit.assertTrue(lockManager.isLocked(key));
      AssertJUnit.assertTrue(lockManager.ownsLock(key, lockOwner1));

      LockPromise lockPromise = lockManager.lockAll(Arrays.asList(key, key2, key3), lockOwner2, 1, TimeUnit.MINUTES);

      AssertJUnit.assertFalse(lockPromise.isAvailable());

      lockManager.unlock(key, lockOwner1);

      AssertJUnit.assertTrue(lockPromise.isAvailable());
      lockPromise.lock();

      AssertJUnit.assertEquals(lockOwner2, lockManager.getOwner(key));
      AssertJUnit.assertTrue(lockManager.isLocked(key));
      AssertJUnit.assertTrue(lockManager.ownsLock(key, lockOwner2));

      lockManager.unlockAll(Arrays.asList(key, key2, key3), lockOwner2);
      AssertJUnit.assertEquals(0, lockManager.getNumberOfLocksHeld());
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
