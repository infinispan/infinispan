package org.infinispan.profiling;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Test for benchmarking the performance of deadlock detection code. Performance is measured as number of successful
 * transactions per minute.
 * <pre>
 * Test description:
 *    We use a fixed size pool of keys ({@link #KEY_POOL_SIZE}) on which each transaction operates. A number of threads ({@link #THREAD_COUNT})
 * repeatedly starts transactions and tries to acquire locks on a random subset of this pool, by executing put
 * operations on each key. If all locks were successfully acquired then the tx tries to commit: only if it succeeds this tx is counted as successful.
 * The number of elements in this subset is the transaction size ({@link #TX_SIZE}). The greater transaction
 * size is, the higher chance for deadlock situation to occur.
 * On each thread these transactions are being repeatedly executed (each time on a different, random key set) for a given time
 * interval ({@link #BENCHMARK_DURATION}). At the end, the number of successful transactions from each thread is cumulated, and this
 * defines throughput (successful tx) per time unit (by default one minute).
 * </pre>
 * There are two different benchmark methods, one for local cache {@link #testLocalDifferentTxSize()} and one for replicated caches
 * {@link #testReplDifferentTxSize()}.
 *
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "profiling", enabled = true, testName = "profiling.DeadlockDetectionPerformanceTest")
public class DeadlockDetectionPerformanceTest extends AbstractInfinispanTest {

   private static final Log log = LogFactory.getLog(DeadlockDetectionPerformanceTest.class);

   public static final int KEY_POOL_SIZE = 10;

   public static int TX_SIZE = 5;

   public static int THREAD_COUNT = 5;

   public static final long BENCHMARK_DURATION = 60000;

   public static boolean NO_COLISION = false;

   public static boolean USE_DLD = true;

   public static List<String> keyPool;

   @BeforeTest
   public void generateKeyPool() {
      keyPool = new ArrayList<String>();
      for (int i = 0; i < KEY_POOL_SIZE; i++) {
         keyPool.add("key" + i);
      }
   }

   @Test(invocationCount = 10)
   public void testLocalDifferentTxSize() throws Exception {
      USE_DLD = false;
      for (int i = 2; i < KEY_POOL_SIZE; i++) {
         TX_SIZE = i;
         runLocalTest();
      }
      USE_DLD = true;
      for (int i = 2; i < KEY_POOL_SIZE; i++) {
         TX_SIZE = i;
         runLocalTest();
      }
   }

   @Test(invocationCount = 5)
   public void testLocalDifferentTxSizeNoCollision() throws Exception {
      NO_COLISION = true;
      USE_DLD = false;
      for (int i = 2; i < KEY_POOL_SIZE; i++) {
         TX_SIZE = i;
         runLocalTest();
      }
      USE_DLD = true;
      for (int i = 2; i < KEY_POOL_SIZE; i++) {
         TX_SIZE = i;
         runLocalTest();
      }
   }

   @Test(invocationCount = 10)
   public void testReplDifferentTxSize() throws Exception {
      THREAD_COUNT = 2;
      USE_DLD = false;
      for (int i = 2; i < KEY_POOL_SIZE; i++) {
         TX_SIZE = i;
         runDistributedTest();
      }
      USE_DLD = true;
      for (int i = 2; i < KEY_POOL_SIZE; i++) {
         TX_SIZE = i;
         runDistributedTest();
      }
   }

   @Test(invocationCount = 5)
   public void testReplDifferentTxSizeDldOnly() throws Exception {
      THREAD_COUNT = 3;
      USE_DLD = true;
      for (int i = 2; i < KEY_POOL_SIZE; i++) {
         TX_SIZE = i;
         runDistributedTest();
      }
   }

   private void runDistributedTest() throws Exception {
      EmbeddedCacheManager cm = null;
      List<EmbeddedCacheManager> containers = new ArrayList<EmbeddedCacheManager>();
      try {
         CountDownLatch startLatch = new CountDownLatch(1);
         List<ExecutorThread> executorThreads = new ArrayList<ExecutorThread>();
         for (int i = 0; i < THREAD_COUNT; i++) {
            cm = TestCacheManagerFactory.createClusteredCacheManager();
            ConfigurationBuilder configuration = getConfiguration();
            configuration.clustering().cacheMode(CacheMode.REPL_SYNC);
            cm.defineConfiguration("test", configuration.build());
            Cache distCache = cm.getCache("test");
            ExecutorThread executorThread = new ExecutorThread(startLatch, distCache);
            executorThreads.add(executorThread);
            containers.add(cm);
         }
         TestingUtil.blockUntilViewsReceived(10000, containers.toArray(new CacheContainer[containers.size()]));
         startLatch.countDown();
         Thread.sleep(BENCHMARK_DURATION);
         joinThreadsAndPrintResult(executorThreads);
      } finally {
         log.trace("About to kill cache managers: " + containers);
         TestingUtil.killCacheManagers(containers);
      }
   }

   private void runLocalTest() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(false);
      try {
         ConfigurationBuilder configuration = getConfiguration();
         cm.defineConfiguration("test", configuration.build());
         Cache localCache = cm.getCache("test");

         CountDownLatch startLatch = new CountDownLatch(1);

         List<ExecutorThread> executorThreads = new ArrayList<ExecutorThread>();
         for (int i = 0; i < THREAD_COUNT; i++) {
            ExecutorThread executorThread = new ExecutorThread(startLatch, localCache);
            executorThreads.add(executorThread);
         }
         startLatch.countDown();
         Thread.sleep(BENCHMARK_DURATION);
         joinThreadsAndPrintResult(executorThreads);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   private void joinThreadsAndPrintResult(List<ExecutorThread> executorThreads) throws InterruptedException {
      int totalSuccess = 0;
      int totalFailures = 0;
      for (int i = 0; i < THREAD_COUNT; i++) {
         ExecutorThread executorThread = executorThreads.get(i);
         executorThread.join();
         totalSuccess += executorThread.getSuccessfullTx();
         totalFailures += executorThread.getFailedTx();
      }
      System.out.println("Use DDL? " + USE_DLD + " TX_SIZE = " + TX_SIZE + " totalSuccess = " + totalSuccess);
      System.out.println("Use DDL? " + USE_DLD + " TX_SIZE = " + TX_SIZE + " totalFailures = " + totalFailures);
      System.out.println("-------------------------------");
   }

   private ConfigurationBuilder getConfiguration() {
      ConfigurationBuilder configuration = new ConfigurationBuilder();
      configuration.transaction().transactionManagerLookup(new DummyTransactionManagerLookup()).deadlockDetection().enabled(USE_DLD).locking().useLockStriping(false);
      return configuration;
   }

   public static class ExecutorThread extends Thread {
      private volatile CountDownLatch startLatch;
      private volatile int successfullTx;
      private volatile int failedTx;
      private volatile Cache cache;
      private volatile TransactionManager txManager;
      static int TX_INDEX = 0;

      public ExecutorThread(CountDownLatch startLatch, Cache cache) {
         super("EXECUTOR-THREAD-" + TX_INDEX++);
         this.startLatch = startLatch;
         this.cache = cache;
         txManager = TestingUtil.getTransactionManager(cache);
         start();
      }

      @Override
      public void run() {
         long start = System.currentTimeMillis();
         try {
            startLatch.await();
         } catch (InterruptedException e) {
            return;
         }
         while ((start + BENCHMARK_DURATION) - System.currentTimeMillis() > 0) {
            try {
               txManager.begin();
               List<String> keysToUpdate = getKeysPerTx();
               for (String key : keysToUpdate) {
                  cache.put(key, "value");
               }
               txManager.commit();
               successfullTx++;
            } catch (Throwable e) {
               failedTx++;
            }
         }
         info("Exiting thread " + getName() + " which lived " + (System.currentTimeMillis() - start) + " milliseconds");
      }

      private void info(String s) {
         System.out.println( "[" + getName() + "] " + s);
         log.trace(s);
      }

      public int getFailedTx() {
         return failedTx;
      }

      public int getSuccessfullTx() {
         return successfullTx;
      }
   }

   private static List<String> getKeysPerTx() {
      Random rnd = new Random();
      Set<String> result = new HashSet<String>();
      while (result.size() < TX_SIZE) {
         String key = keyPool.get(rnd.nextInt(KEY_POOL_SIZE));
         result.add(key);
      }
      ArrayList resultList = new ArrayList(result);
      if (!NO_COLISION) {
         Collections.shuffle(resultList);
      }
      return resultList;
   }
}
