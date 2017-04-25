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
package org.infinispan.stress;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

/**
 * Verifies the atomic semantic of Infinispan's implementations of
 * java.util.concurrent.ConcurrentMap<K, V>.replace(Object, Object, Object); which is an interesting
 * concurrent locking case.
 *
 * The test is quite complex to read, might be worth explaining the rather simple strategy:
 * we setup NODES_NUM cache nodes in various combinations of clustering modes and transactional
 * modes, and will use THREADS parallel workers.
 * We pick a single key SHARED_KEY, which is the only key all workers will use to read
 * and write.
 * Each thread will so some preparation work, then wait for a shared barrier; when the barrier
 * is reached they will all attempt their own replace operation. After this, they will share
 * and verify outcomes. Because of this design the pre- and post- operations and validations
 * don't need to be particularly efficient and for sake of convenience we use plain synchronization.
 *
 * More in detail: the preparation for each thread consists in reading the current value for
 * the shared key and picking a "next value" which is unique for that thread.
 * These conditions make it easier to validate the semantics, as one and only one thread should
 * see a "true" being returned from the replace operation, and the value written by that thread
 * is the new value we expect to find in the cache.
 *
 * @since 5.2
 * @see java.util.concurrent.ConcurrentMap#replace(Object, Object, Object)
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2012 Red Hat Inc.
 */
@Test(groups = "stress", testName = "stress.replaceOperationtressTest", enabled = true,
      description = "Since this test is slow to run, it should be disabled by default and run by hand as necessary.")
public class ReplaceOperationStressTest extends AbstractInfinispanTest {

   private static final int NODES_NUM = 5;
   private static final int MOVES = 200_000;
   private static final int THREADS = 10;
   private static final String SHARED_KEY = "thisIsTheKeyForConcurrentAccess";

   private static final String[] validMoves = generateValidMoves();

   private static final AtomicBoolean failed = new AtomicBoolean(false);
   private static final AtomicBoolean quit = new AtomicBoolean(false);
   private static final AtomicInteger liveWorkers = new AtomicInteger();
   private static volatile String failureMessage = "";

   /**
    * Testing replace(Object, Object, Object) behaviour on clustered DIST_SYNC caches
    */
   public void testonInfinispanDIST_SYNC_NonTX() throws Exception {
      testEntry(CacheMode.DIST_SYNC, false);
   }

   /**
    * Testing replace(Object, Object, Object) behaviour on clustered DIST_SYNC caches with transactions
    */
   public void testonInfinispanDIST_SYNC_TX() throws Exception {
      testEntry(CacheMode.DIST_SYNC, true);
   }

   /**
    * Testing replace(Object, Object, Object) behaviour on LOCAL caches
    */
   public void testonInfinispanLOCAL_NonTX() throws Exception {
      testEntry(CacheMode.LOCAL, false);
   }

   /**
    * Testing replace(Object, Object, Object) behaviour on LOCAL caches with transactions
    */
   public void testonInfinispanLOCAL_TX() throws Exception {
      testEntry(CacheMode.LOCAL, true);
   }

   /**
    * Testing replace(Object, Object, Object) behaviour on clustered REPL_SYNC caches
    */
   public void testonInfinispanREPL_NonTX() throws Exception {
      testEntry(CacheMode.REPL_SYNC, false);
   }

   /**
    * Testing replace(Object, Object, Object) behaviour on clustered REPL_SYNC caches with transactions
    */
   public void testonInfinispanREPL_TX() throws Exception {
      testEntry(CacheMode.REPL_SYNC, true);
   }

   private void testEntry(final CacheMode mode, final boolean transactional) {
      final ConfigurationBuilder builder;
      if (mode.isClustered()) {
         builder = AbstractCacheTest.getDefaultClusteredCacheConfig(mode, transactional);
      }
      else {
         builder = TestCacheManagerFactory.getDefaultCacheConfiguration(transactional);
      }
      testOnConfiguration(builder, NODES_NUM, mode);
   }

   private void testOnConfiguration(final ConfigurationBuilder builder, final int nodesNum, final CacheMode mode) {
      final List<EmbeddedCacheManager> cacheManagers;
      final List<Cache> caches;
      if (mode.isClustered()) {
         cacheManagers = new ArrayList<EmbeddedCacheManager>(nodesNum);
         caches = new ArrayList<Cache>(nodesNum);
         for (int i = 0; i < nodesNum; i++) {
            EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createClusteredCacheManager(builder);
            cacheManagers.add(cacheManager);
            caches.add(cacheManager.getCache());
         }
         TestingUtil.blockUntilViewsReceived(10000, caches);
         if (mode.isDistributed()) {
            TestingUtil.waitForRehashToComplete(caches);
         }
      }
      else {
         EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(builder);
         cacheManagers = Collections.singletonList(cacheManager);
         Cache cache = cacheManager.getCache();
         caches = Collections.singletonList(cache);
      }
      try {
         testOnCaches(caches);
      }
      finally {
         TestingUtil.killCaches(caches);
         TestingUtil.killCacheManagers(cacheManagers);
      }
   }

   private void testOnCaches(List<Cache> caches) {
      failed.set(false);
      quit.set(false);
      caches.get(0).put(SHARED_KEY, validMoves[0]);
      final SharedState state = new SharedState(THREADS);
      final PostOperationStateCheck stateCheck = new PostOperationStateCheck(caches, state);
      final CyclicBarrier barrier = new CyclicBarrier(THREADS, stateCheck);
      ExecutorService exec = Executors.newFixedThreadPool(THREADS);
      for (int threadIndex = 0; threadIndex < THREADS; threadIndex++) {
         Runnable validMover = new ValidMover(caches, barrier, threadIndex, state);
         exec.execute(validMover);
      }
      exec.shutdown();
      try {
         exec.awaitTermination(1, TimeUnit.DAYS);
      } catch (InterruptedException e) {
         e.printStackTrace();
         assert false : e.getMessage();
      }
      assert !failed.get() : failureMessage;
   }

   private static String[] generateValidMoves() {
      String[] validMoves = new String[MOVES];
      for ( int i=0; i<MOVES; i++) {
         validMoves[i] = "v_"+i;
      }
      System.out.println("Valid moves ready");
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

      public ValidMover(List<Cache> caches, CyclicBarrier barrier, int threadIndex, SharedState state) {
         this.caches = caches;
         this.barrier = barrier;
         this.threadIndex = threadIndex;
         this.state = state;
      }

      @Override
      public void run() {
         int cachePickIndex = threadIndex;
         liveWorkers.incrementAndGet();
         try {
            for (int moveToIndex = threadIndex;
                  (moveToIndex < validMoves.length) && (!barrier.isBroken() && (!failed.get()) && !quit.get());
                  moveToIndex += THREADS) {
               cachePickIndex = ++cachePickIndex % caches.size();
               Cache cache = caches.get(cachePickIndex);
               Object expected = cache.get(SHARED_KEY);
               String targetValue = validMoves[moveToIndex];
               state.beforeReplace(threadIndex, expected, targetValue);
               barrier.await();
               boolean replaced = cache.replace(SHARED_KEY, expected, targetValue);
               state.afterReplace(threadIndex, expected, targetValue, replaced);
               barrier.await();
            }
            //not all threads might finish at the same block, so make sure noone stays waiting for us when we exit
            quit.set(true);
            barrier.reset();
         } catch (InterruptedException e) {
            fail(e);
         } catch (BrokenBarrierException e) {
            //just quit
            System.out.println("Broken barrier!");
         } catch (RuntimeException e) {
            fail(e);
         }
         finally {
            int andGet = liveWorkers.decrementAndGet();
            barrier.reset();
            System.out.println("Thread #" + threadIndex + " terminating. Still " + andGet + " threads alive");
         }
      }
   }

   static final class SharedState {
      private final SharedThreadState[] threadstates;
      private boolean after = false;

      public SharedState(final int threads) {
         threadstates = new SharedThreadState[threads];
         for (int i = 0; i < threads; i++) {
            threadstates[i] = new SharedThreadState();
         }
      }

      synchronized void beforeReplace(int threadIndex, Object expected, String targetValue) {
         threadstates[threadIndex].beforeReplace(expected, targetValue);
         after = false;
      }

      synchronized void afterReplace(int threadIndex, Object expected, String targetValue, boolean replace) {
         threadstates[threadIndex].afterReplace(expected, targetValue, replace);
         after = true;
      }

      synchronized boolean isAfter() {
         return after;
      }

   }

   static final class SharedThreadState {
      volatile Object beforeExpected;
      volatile Object beforeTargetValue;
      volatile Object afterExpected;
      volatile Object afterTargetValue;
      volatile boolean successfullyReplaced;
      public void beforeReplace(Object expected, Object targetValue) {
         this.beforeExpected = expected;
         this.beforeTargetValue = targetValue;
      }
      public void afterReplace(Object expected, Object targetValue, boolean replaced) {
         this.afterExpected = expected;
         this.afterTargetValue = targetValue;
         this.successfullyReplaced = replaced;
      }
   }

   static final class PostOperationStateCheck implements Runnable {

      private final List<Cache> caches;
      private final SharedState state;
      private final AtomicInteger cycle = new AtomicInteger();

      public PostOperationStateCheck(final List<Cache> caches, final SharedState state) {
         this.caches = caches;
         this.state = state;
      }

      @Override
      public void run() {
         if ( state.isAfter() ) {
            int c = cycle.incrementAndGet();
            if (c % (MOVES / 100) == 0) {
               System.out.println( ( c * 100 * THREADS / MOVES ) + "%");
            }
            checkAfterState();
         }
         else {
            checkBeforeState();
         }
      }

      private void checkBeforeState() {
         final Object currentStored = caches.get(0).get(SHARED_KEY);
         for (Cache c : caches) {
            if (!currentStored.equals(c.get(SHARED_KEY))) {
               fail("Precondition failure: not all caches are storing the same value");
            }
         }
         for (SharedThreadState threadState : state.threadstates) {
            if ( !threadState.beforeExpected.equals( currentStored ) ) {
               fail( "Some cache expected a different value than what is stored" );
            }
         }
      }

      private void checkAfterState() {
         AdvancedCache someCache = caches.get(0).getAdvancedCache();
         final Object currentStored = someCache.get(SHARED_KEY);
         HashSet uniqueValueVerify = new HashSet();
         for (SharedThreadState threadState : state.threadstates) {
            uniqueValueVerify.add(threadState.afterTargetValue);
         }
         if (uniqueValueVerify.size()!=THREADS) {
            fail("test bug! Workers aren't attempting to write different values");
         }
         {
            int replaced = 0;
            for (SharedThreadState threadState : state.threadstates) {
               if (threadState.successfullyReplaced) {
                  replaced++;
               }
            }
            if (replaced!=1) {
               fail(replaced + " threads assume a succesfull replacement! (CAS should succeed on a single thread only)");
            }
         }
         for (SharedThreadState threadState : state.threadstates) {
            if (threadState.successfullyReplaced) {
               if (! threadState.afterTargetValue.equals(currentStored)) {
                  fail("replace successful but the current stored value doesn't match the write operation of the successful thread");
               }
            }
            else {
               if (threadState.afterTargetValue.equals(currentStored)) {
                  fail("replace not successful (which is fine) but the current stored value matches the write attempt");
               }
            }
         }
         for (Cache c : caches) {
            LockManager lockManager = c.getAdvancedCache().getComponentRegistry().getComponent(LockManager.class);
            if (lockManager.isLocked(SHARED_KEY)) {
               fail("lock on the entry wasn't cleaned up");
            }
         }
      }
   }

}
