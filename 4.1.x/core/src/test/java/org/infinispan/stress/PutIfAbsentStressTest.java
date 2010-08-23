/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies the atomic semantic of Infinispan's implementations of
 * java.util.concurrent.ConcurrentMap<K, V>.putIfAbsent(K key, V value); which is an interesting
 * concurrent locking case.
 * 
 * @since 4.0
 * @see java.util.concurrent.ConcurrentMap#putIfAbsent(Object, Object)
 * @author Sanne Grinovero
 */
@Test(groups = "stress", testName = "stress.PutIfAbsentStressTest", enabled = false,
      description = "Since this test is slow to run, it should be disabled by default and run by hand as necessary.")
public class PutIfAbsentStressTest {

   private static final int NODES_NUM = 5;
   private static final int THREAD_PER_NODE = 20;
   private static final long STRESS_TIME_MINUTES = 1;
   private static final long SLEEP_MILLISECONDS = 50;
   private static final String SHARED_KEY = "thisIsTheKeyForConcurrentAccess";

   /**
    * Purpose is not testing JDK's ConcurrentHashMap but ensuring the test is correct. It's also
    * interesting to compare performance.
    */
   protected void testonConcurrentHashMap() throws Exception {
      ConcurrentMap<String, String> map = new ConcurrentHashMap<String, String>();
      testConcurrentLocking(map);
   }

   /**
    * Testing putIfAbsent's behaviour on a Local cache.
    */
   protected void testonInfinispanLocal() throws Exception {
      CacheContainer cm = TestCacheManagerFactory.createLocalCacheManager(false);
      ConcurrentMap<String, String> map = cm.getCache();
      try {
         testConcurrentLocking(map);
      } finally {
         TestingUtil.clearContent(cm);
      }
   }

   /**
    * Testing putIfAbsent's behaviour in DIST_SYNC cache.
    */
   protected void testonInfinispanDIST() throws Exception {
      Configuration c = new Configuration();
      c.setCacheMode(Configuration.CacheMode.DIST_SYNC);
      testConcurrentLockingOnMultipleManagers(c);
   }

   /**
    * Testing putIfAbsent's behaviour in REPL_SYNC cache.
    */
   protected void testonInfinispanREPL() throws Exception {
      Configuration c = new Configuration();
      c.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      testConcurrentLockingOnMultipleManagers(c);
   }

   /**
    * Adapter to run the test on any configuration
    */
   private void testConcurrentLockingOnMultipleManagers(Configuration cfg) throws IOException, InterruptedException {
      List<CacheContainer> cacheContainers = new ArrayList<CacheContainer>(NODES_NUM);
      List<Cache<String, String>> caches = new ArrayList<Cache<String, String>>();
      List<ConcurrentMap<String, String>> maps = new ArrayList<ConcurrentMap<String, String>>(NODES_NUM
               * THREAD_PER_NODE);
      for (int nodeNum = 0; nodeNum < NODES_NUM; nodeNum++) {
         CacheContainer cm = TestCacheManagerFactory.createClusteredCacheManager(cfg);
         cacheContainers.add(cm);
         Cache<String, String> cache = cm.getCache();
         caches.add(cache);
         for (int threadNum = 0; threadNum < THREAD_PER_NODE; threadNum++) {
            maps.add(cache);
         }
      }
      TestingUtil.blockUntilViewsReceived(10000, caches);
      try {
         testConcurrentLocking(maps);
      } finally {
         for (CacheContainer cm : cacheContainers) {
            try {
               TestingUtil.killCacheManagers(cm);
            } catch (Exception e) {
               // try cleaning up the other cacheManagers too
            }
         }
      }
   }

   /**
    * Adapter for tests sharing a single Cache instance
    */
   private void testConcurrentLocking(ConcurrentMap<String, String> map) throws IOException, InterruptedException {
      int size = NODES_NUM * THREAD_PER_NODE;
      List<ConcurrentMap<String, String>> maps = new ArrayList<ConcurrentMap<String, String>>(size);
      for (int i = 0; i < size; i++) {
         maps.add(map);
      }
      testConcurrentLocking(maps);
   }

   /**
    * Drives the actual test on an Executor and verifies the result
    * 
    * @param maps the caches to be tested
    * @throws IOException
    * @throws InterruptedException
    */
   private void testConcurrentLocking(List<ConcurrentMap<String, String>> maps) throws IOException,
            InterruptedException {
      SharedStats stats = new SharedStats();
      ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(NODES_NUM);
      List<StressingThread> threads = new ArrayList<StressingThread>();
      for (ConcurrentMap<String, String> map : maps) {
         StressingThread thread = new StressingThread(stats, map);
         threads.add(thread);
         executor.execute(thread);
      }
      executor.shutdown();
      Thread.sleep(5000);
      int putsAfter5Seconds = stats.succesfullPutsCounter.get();
      System.out.println("\nSituation after 5 seconds:");
      System.out.println(stats.toString());
      executor.awaitTermination(STRESS_TIME_MINUTES, TimeUnit.MINUTES);
      stats.globalQuit = true;
      executor.awaitTermination(10, TimeUnit.SECONDS); // give some time to awake and quit
      executor.shutdownNow();
      System.out.println("\nFinal situation:");
      System.out.println(stats.toString());
      assert !stats.seenFailures : "at least one thread has seen unexpected state";
      assert stats.succesfullPutsCounter.get() > 0 : "the lock should have been taken at least once";
      assert stats.succesfullPutsCounter.get() > putsAfter5Seconds : "the lock count didn't improve since the first 5 seconds. Deadlock?";
      assert stats.succesfullPutsCounter.get() == stats.lockReleasedCounter.get() : "there's a mismatch in acquires and releases count";
      assert stats.lockOwnersCounter.get() == 0 : "the lock is still held at test finish";
   }

   private static class StressingThread implements Runnable {

      private final SharedStats stats;
      private final ConcurrentMap<String, String> cache;

      public StressingThread(SharedStats stats, ConcurrentMap<String, String> cache) {
         this.stats = stats;
         this.cache = cache;
      }

      @Override
      public void run() {
         while (!(stats.seenFailures || stats.globalQuit || Thread.interrupted())) {
            try {
               doCycle();
            } catch (IOException e) {
               checkIsTrue(false, e.getMessage());
            }
         }
      }

      private void doCycle() throws IOException {
         String beforePut = cache.putIfAbsent(SHARED_KEY, SHARED_KEY);
         if (beforePut != null) {
            stats.canceledPutsCounter.incrementAndGet();
            sleep();
         } else {
            boolean lockIsFine = stats.lockOwnersCounter.compareAndSet(0, 1);
            System.out.print("L");
            stats.succesfullPutsCounter.incrementAndGet();
            checkIsTrue(lockIsFine, "I got the lock, some other thread is owning the lock AS WELL.");
            sleep();
            lockIsFine = stats.lockOwnersCounter.compareAndSet(1, 0);
            checkIsTrue(lockIsFine, "Some other thread changed the lock count while I was having it!");
            System.out.print("R");
            cache.remove(SHARED_KEY);
            stats.lockReleasedCounter.incrementAndGet();
         }
      }

      private void sleep() {
         try {
            Thread.sleep(SLEEP_MILLISECONDS);
         } catch (InterruptedException e) {
            // no-op: waking up is good enough
         }
      }

      private void checkIsTrue(boolean assertion, String message) {
         if (assertion == false) {
            stats.seenFailures = true;
            System.out.println(message);
         }
      }

   }

   /**
    * Common state to verify cache behaviour
    */
   public static class SharedStats {

      final AtomicInteger canceledPutsCounter = new AtomicInteger(0);
      final AtomicInteger succesfullPutsCounter = new AtomicInteger(0);
      final AtomicInteger lockReleasedCounter = new AtomicInteger(0);
      final AtomicInteger lockOwnersCounter = new AtomicInteger(0);
      Throwable throwable = null;
      volatile boolean globalQuit = false; // when it's true the threads quit
      volatile boolean seenFailures = false; // set to true by a thread if it has experienced
                                             // illegal state

      public String toString() {
         return "\n\tCanceled puts count:\t" + canceledPutsCounter.get() +
                "\n\tSuccesfull puts count:\t" + succesfullPutsCounter.get() +
                "\n\tRemoved count:\t" + lockReleasedCounter.get() +
                "\n\tIllegal state detected:\t" + seenFailures;
      }

   }

}
