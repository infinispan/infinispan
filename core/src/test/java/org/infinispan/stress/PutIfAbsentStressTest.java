package org.infinispan.stress;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

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
   private static final int THREAD_PER_NODE = 12;
   private static final long STRESS_TIME_MINUTES = 2;
   private static final String SHARED_KEY = "thisIsTheKeyForConcurrentAccess";

   /**
    * Purpose is not testing JDK's ConcurrentHashMap but ensuring the test is correct. It's also
    * interesting to compare performance.
    */
   public void testonConcurrentHashMap() throws Exception {
      System.out.println("Running test on ConcurrentHashMap:");
      ConcurrentMap<String, String> map = new ConcurrentHashMap<String, String>();
      testConcurrentLocking(map);
   }

   /**
    * Testing putIfAbsent's behaviour on a Local cache.
    */
   public void testonInfinispanLocal() throws Exception {
      System.out.println("Running test on Infinispan, LOCAL:");
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(false);
      ConcurrentMap<String, String> map = cm.getCache();
      try {
         testConcurrentLocking(map);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   /**
    * Testing putIfAbsent's behaviour in DIST_SYNC cache.
    */
   public void testonInfinispanDIST_SYNC() throws Exception {
      System.out.println("Running test on Infinispan, DIST_SYNC:");
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.DIST_SYNC);
      testConcurrentLockingOnMultipleManagers(c);
   }

   /**
    * Testing putIfAbsent's behaviour in DIST_SYNC cache, disabling L1
    */
   public void testonInfinispanDIST_NOL1() throws Exception {
      System.out.println("Running test on Infinispan, DIST_SYNC, disabling L1:");
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.DIST_SYNC).l1().disable();
      testConcurrentLockingOnMultipleManagers(c);
   }

   /**
    * Testing putIfAbsent's behaviour in REPL_SYNC cache.
    */
   public void testonInfinispanREPL_SYNC() throws Exception {
      System.out.println("Running test on Infinispan, REPL_SYNC:");
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.REPL_SYNC);
      testConcurrentLockingOnMultipleManagers(c);
   }

   /**
    * Testing putIfAbsent's behaviour in REPL_ASYNC cache.
    */
   public void testonInfinispanREPL_ASYNC() throws Exception {
      System.out.println("Running test on Infinispan, REPL_ASYNC:");

      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.REPL_ASYNC);
      testConcurrentLockingOnMultipleManagers(c);
   }

   /**
    * Adapter to run the test on any configuration
    */
   private void testConcurrentLockingOnMultipleManagers(ConfigurationBuilder cfg) throws InterruptedException {
      List<EmbeddedCacheManager> cacheContainers = new ArrayList<EmbeddedCacheManager>(NODES_NUM);
      List<Cache<String, String>> caches = new ArrayList<Cache<String, String>>();
      List<ConcurrentMap<String, String>> maps = new ArrayList<ConcurrentMap<String, String>>(NODES_NUM
               * THREAD_PER_NODE);
      for (int nodeNum = 0; nodeNum < NODES_NUM; nodeNum++) {
         EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(cfg);
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
         TestingUtil.killCacheManagers(cacheContainers);
      }
   }

   /**
    * Adapter for tests sharing a single Cache instance
    */
   private void testConcurrentLocking(ConcurrentMap<String, String> map) throws InterruptedException {
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
    */
   private void testConcurrentLocking(List<ConcurrentMap<String, String>> maps) throws InterruptedException {
      SharedStats stats = new SharedStats();
      ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(NODES_NUM);
      List<StressingThread> threads = new ArrayList<StressingThread>();
      int i=0;
      for (ConcurrentMap<String, String> map : maps) {
         StressingThread thread = new StressingThread(stats, map, i++);
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
      private final String ourValue;

      public StressingThread(SharedStats stats, ConcurrentMap<String, String> cache, int threadId) {
         this.stats = stats;
         this.cache = cache;
         this.ourValue = "v" + threadId;
      }

      @Override
      public void run() {
         while (!(stats.seenFailures || stats.globalQuit || Thread.interrupted())) {
               doCycle();
         }
      }

      private void doCycle() {
         String beforePut = cache.putIfAbsent(SHARED_KEY, ourValue);
         if (beforePut != null) {
            stats.canceledPutsCounter.incrementAndGet();
         } else {
            final String currentCacheValue = cache.get(SHARED_KEY);
            boolean lockIsFine = stats.lockOwnersCounter.compareAndSet(0, 1) && ourValue.equals(currentCacheValue);
            stats.succesfullPutsCounter.incrementAndGet();
            checkIsTrue(lockIsFine, "I got the lock, some other thread is owning the lock AS WELL.");
            lockIsFine = stats.lockOwnersCounter.compareAndSet(1, 0);
            checkIsTrue(lockIsFine, "Some other thread changed the lock count while I was having it!");
            cache.remove(SHARED_KEY);
            stats.lockReleasedCounter.incrementAndGet();
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

      @Override
      public String toString() {
         return "\n\tCanceled puts count:\t" + canceledPutsCounter.get() +
                "\n\tSuccesfull puts count:\t" + succesfullPutsCounter.get() +
                "\n\tRemoved count:\t" + lockReleasedCounter.get() +
                "\n\tIllegal state detected:\t" + seenFailures;
      }

   }

}
