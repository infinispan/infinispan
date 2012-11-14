/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.api;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies the atomic semantic of Infinispan's implementations of java.util.concurrent.ConcurrentMap'
 * conditional operations.
 *
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2012 Red Hat Inc.
 * @see java.util.concurrent.ConcurrentMap#replace(Object, Object, Object)
 * @since 5.2
 */
@Test(groups = "functional", testName = "api.replaceOperationtressTest", enabled = true)
public class ConditionalOperationsConcurrentTest extends MultipleCacheManagersTest {

   private static Log log = LogFactory.getLog(ConditionalOperationsConcurrentTest.class);

   private static boolean ENABLE_DEBUG = false;

   private static final int NODES_NUM = 3;
   private static final int MOVES = 1000;
   private static final int THREAD_COUNT = 4;
   private static final String SHARED_KEY = "thisIsTheKeyForConcurrentAccess";

   private static final String[] validMoves = generateValidMoves();

   private static final AtomicBoolean failed = new AtomicBoolean(false);
   private static final AtomicBoolean quit = new AtomicBoolean(false);
   private static final AtomicInteger liveWorkers = new AtomicInteger();
   private static volatile String failureMessage = "";

   private boolean transactional = false;
   private CacheMode mode = CacheMode.DIST_SYNC;

   @BeforeMethod
   public void init() {
      failed.set(false);
      quit.set(false);
      liveWorkers.set(0);
      failureMessage = "";
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(mode, transactional);
      createCluster(dcc, NODES_NUM);
      waitForClusterToForm();
   }

   public void testReplace() throws Exception {
      List caches = caches(null);
      testOnCaches(caches, new ReplaceOperation());
   }

   public void testConditionalRemove() throws Exception {
      List caches = caches(null);
      testOnCaches(caches, new ConditionalRemoveOperation());
   }

   public void testPutIfAbsent() throws Exception {
      List caches = caches(null);
      testOnCaches(caches, new PutIfAbsentOperation());
   }

   private void testOnCaches(List<Cache> caches, CacheOperation operation) {
      failed.set(false);
      quit.set(false);
      caches.get(0).put(SHARED_KEY, "initialValue");
      final SharedState state = new SharedState(THREAD_COUNT);
      final PostOperationStateCheck stateCheck = new PostOperationStateCheck(caches, state, operation);
      final CyclicBarrier barrier = new CyclicBarrier(THREAD_COUNT, stateCheck);
      ExecutorService exec = Executors.newFixedThreadPool(THREAD_COUNT);
      for (int threadIndex = 0; threadIndex < THREAD_COUNT; threadIndex++) {
         Runnable validMover = new ValidMover(caches, barrier, threadIndex, state, operation);
         exec.execute(validMover);
      }
      exec.shutdown();
      try {
         exec.awaitTermination(5, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
         e.printStackTrace();
         assert false : e.getMessage();
      }
      assert !failed.get() : failureMessage;
   }

   private static String[] generateValidMoves() {
      String[] validMoves = new String[MOVES];
      for (int i = 0; i < MOVES; i++) {
         validMoves[i] = "v_" + i;
      }
      print("Valid moves ready");
      return validMoves;
   }

   private static void fail(final String message) {
      boolean firstFailure = failed.compareAndSet(false, true);
      if (firstFailure) {
         failureMessage = message;
      }
   }

   private static void fail(final Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      fail(sw.toString());
   }

   static final class ValidMover implements Runnable {

      private final List<Cache> caches;
      private final int threadIndex;
      private final CyclicBarrier barrier;
      private final SharedState state;
      private final CacheOperation operation;

      public ValidMover(List<Cache> caches, CyclicBarrier barrier, int threadIndex, SharedState state, CacheOperation operation) {
         this.caches = caches;
         this.barrier = barrier;
         this.threadIndex = threadIndex;
         this.state = state;
         this.operation = operation;
      }

      @Override
      public void run() {
         int cachePickIndex = threadIndex;
         liveWorkers.incrementAndGet();
         try {
            for (int moveToIndex = threadIndex;
                 (moveToIndex < validMoves.length) && (!barrier.isBroken() && (!failed.get()) && !quit.get());
                 moveToIndex += THREAD_COUNT) {
               operation.beforeOperation(caches.get(0));

               cachePickIndex = ++cachePickIndex % caches.size();
               Cache cache = caches.get(cachePickIndex);
               Object existing = cache.get(SHARED_KEY);
               String targetValue = validMoves[moveToIndex];
               state.beforeOperation(threadIndex, existing, targetValue);
               blockAtTheBarrier();

               boolean successful = operation.execute(cache, SHARED_KEY, existing, targetValue);
               state.afterOperation(threadIndex, existing, targetValue, successful);
               blockAtTheBarrier();
            }
            //not all threads might finish at the same block, so make sure none stays waiting for us when we exit
            quit.set(true);
            barrier.reset();
         } catch (InterruptedException e) {
            log.error("Caught exception %s", e);
            fail(e);
         } catch (BrokenBarrierException e) {
            log.error("Caught exception %s", e);
            //just quit
            print("Broken barrier!");
         } catch (RuntimeException e) {
            log.error("Caught exception %s", e);
            fail(e);
         } finally {
            int andGet = liveWorkers.decrementAndGet();
            barrier.reset();
            print("Thread #" + threadIndex + " terminating. Still " + andGet + " threads alive");
         }
      }

      private void blockAtTheBarrier() throws InterruptedException, BrokenBarrierException {
         try {
            barrier.await(10000, TimeUnit.MILLISECONDS);
         } catch (TimeoutException e) {
            if (!quit.get()) {
               throw new RuntimeException(e);
            }
         }
      }
   }

   static final class SharedState {
      private final SharedThreadState[] threadStates;
      private volatile boolean after = false;

      public SharedState(final int threads) {
         threadStates = new SharedThreadState[threads];
         for (int i = 0; i < threads; i++) {
            threadStates[i] = new SharedThreadState();
         }
      }

      synchronized void beforeOperation(int threadIndex, Object expected, String targetValue) {
         threadStates[threadIndex].beforeReplace(expected, targetValue);
         after = false;
      }

      synchronized void afterOperation(int threadIndex, Object expected, String targetValue, boolean successful) {
         threadStates[threadIndex].afterReplace(expected, targetValue, successful);
         after = true;
      }

      public boolean isAfter() {
         return after;
      }

   }

   static final class SharedThreadState {
      Object beforeExpected;
      Object beforeTargetValue;
      Object afterExpected;
      Object afterTargetValue;
      boolean successfullOperation;

      public void beforeReplace(Object expected, Object targetValue) {
         this.beforeExpected = expected;
         this.beforeTargetValue = targetValue;
      }

      public void afterReplace(Object expected, Object targetValue, boolean replaced) {
         this.afterExpected = expected;
         this.afterTargetValue = targetValue;
         this.successfullOperation = replaced;
      }

      public boolean sameBeforeValue(Object currentStored) {
         return currentStored == null ? beforeExpected == null : currentStored.equals(beforeExpected);
      }
   }

   static final class PostOperationStateCheck implements Runnable {

      private final List<Cache> caches;
      private final SharedState state;
      private final CacheOperation operation;
      private volatile int cycle = 0;

      public PostOperationStateCheck(final List<Cache> caches, final SharedState state, CacheOperation operation) {
         this.caches = caches;
         this.state = state;
         this.operation = operation;
      }

      @Override
      public void run() {
         if (state.isAfter()) {
            cycle++;
            if (cycle % (MOVES / 100) == 0) {
               print((cycle * 100 * THREAD_COUNT / MOVES) + "%");
            }
            checkAfterState();
         } else {
            checkBeforeState();
         }
      }

      private void checkSameValueOnAllCaches() {
         final Object currentStored = caches.get(0).get(SHARED_KEY);
         log.tracef("Value seen by (first) cache %s is %s ", caches.get(0).getAdvancedCache().getRpcManager().getAddress(),
                    currentStored);
         for (Cache c : caches) {
            Object v = c.get(SHARED_KEY);
            log.tracef("Value seen by cache %s is %s", c.getAdvancedCache().getRpcManager().getAddress(), v);
            boolean sameValue = v == null ? currentStored == null : v.equals(currentStored);
            if (!sameValue) {
               fail("Not all the caches see the same value");
            }
         }
      }

      private void checkBeforeState() {
         final Object currentStored = caches.get(0).get(SHARED_KEY);
         for (SharedThreadState threadState : state.threadStates) {
            if ( !threadState.sameBeforeValue(currentStored)) {
               fail("Some cache expected a different value than what is stored");
            }
         }
      }

      private void checkAfterState() {
         final Object currentStored = assertTestCorrectness();
         checkSameValueOnAllCaches();
         if (operation.isCas()) {
            checkSingleSuccessfulThread();
            checkSuccessfulOperation(currentStored);
         }
         checkNoLocks();
      }

      private Object assertTestCorrectness() {
         AdvancedCache someCache = caches.get(0).getAdvancedCache();
         final Object currentStored = someCache.get(SHARED_KEY);
         HashSet uniqueValueVerify = new HashSet();
         for (SharedThreadState threadState : state.threadStates) {
            uniqueValueVerify.add(threadState.afterTargetValue);
         }
         if (uniqueValueVerify.size() != THREAD_COUNT) {
            fail("test bug");
         }
         return currentStored;
      }

      private void checkNoLocks() {
         for (Cache c : caches) {
            LockManager lockManager = c.getAdvancedCache().getComponentRegistry().getComponent(LockManager.class);
            if (lockManager.isLocked(SHARED_KEY)) {
               fail("lock on the entry wasn't cleaned up");
            }
         }
      }

      private void checkSuccessfulOperation(Object currentStored) {
         for (SharedThreadState threadState : state.threadStates) {
            if (threadState.successfullOperation) {
               if (!operation.validateTargetValueForSuccess(threadState.afterTargetValue, currentStored)) {
                  fail("operation successful but the current stored value doesn't match the write operation of the successful thread");
               }
            } else {
               if (threadState.afterTargetValue.equals(currentStored)) {
                  fail("operation not successful (which is fine) but the current stored value matches the write attempt");
               }
            }
         }
      }

      private void checkSingleSuccessfulThread() {
         //for CAS operations there's only one successful thread
         int successfulThreads = 0;
         for (SharedThreadState threadState : state.threadStates) {
            if (threadState.successfullOperation) {
               successfulThreads++;
            }
         }
         if (successfulThreads != 1) {
            fail(successfulThreads + " threads assume a successful replacement! (CAS should succeed on a single thread only)");
         }
      }
   }

   public static abstract class CacheOperation {
      abstract boolean isCas();

      abstract boolean execute(Cache cache, String sharedKey, Object existing, String targetValue);

      abstract void beforeOperation(Cache cache);

      boolean validateTargetValueForSuccess(Object afterTargetValue, Object currentStored) {
         return afterTargetValue.equals(currentStored);
      }
   }

   static class ReplaceOperation extends CacheOperation {
      @Override
      public boolean isCas() {
         return true;
      }

      @Override
      public boolean execute(Cache cache, String sharedKey, Object existing, String targetValue) {
         return cache.replace(SHARED_KEY, existing, targetValue);
      }

      @Override
      public void beforeOperation(Cache cache) {
      }
   }

   static class PutIfAbsentOperation extends CacheOperation {

      @Override
      public boolean isCas() {
         return true;
      }

      @Override
      public boolean execute(Cache cache, String sharedKey, Object existing, String targetValue) {
         return cache.putIfAbsent(SHARED_KEY, targetValue) == null;
      }

      @Override
      public void beforeOperation(Cache cache) {
         cache.remove(SHARED_KEY);
      }
   }

   static class ConditionalRemoveOperation extends CacheOperation {

      @Override
      public boolean isCas() {
         return true;
      }

      @Override
      public boolean execute(Cache cache, String sharedKey, Object existing, String targetValue) {
         return cache.remove(SHARED_KEY, existing);
      }

      @Override
      public void beforeOperation(Cache cache) {
         cache.put(SHARED_KEY, "someValue");
      }

      @Override
      boolean validateTargetValueForSuccess(Object afterTargetValue, Object currentStored) {
         return currentStored == null;
      }
   }

   private static void print(String s) {
      if (ENABLE_DEBUG) System.out.println(s);
   }
}
