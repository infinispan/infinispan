package org.infinispan.profiling;

import org.infinispan.profiling.testinternals.Generator;
import org.infinispan.profiling.testinternals.TaskRunner;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test to use with a profiler to profile replication.  To be used in conjunction with ProfileSlaveTest.
 * <p/>
 * Typical usage pattern:
 * <p/>
 * 1.  Start a single test method in ProfileSlaveTest.  This will block until you kill it. 2.  Start the corresponding
 * test in this class, with the same name, in a different JVM, and attached to a profiler. 3.  Profile away!
 * <p/>
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 */
@Test(groups = "profiling", enabled = false, testName = "profiling.ProfileTest")
public class ProfileTest extends AbstractProfileTest {
   /*
      Test configuration flags
    */
   protected static long NUM_OPERATIONS = 1000000; // DURATION is replaced with a fixed number of operations instead.
   protected static final int NUM_THREADS = 25;
   protected static final int MAX_RANDOM_SLEEP_MILLIS = 1;
   protected static final int MAX_OVERALL_KEYS = 2000;
   protected static final int WARMUP_LOOPS = 20000;
   protected static final boolean USE_SLEEP = false; // throttle generation a bit
   protected static final boolean SKIP_WARMUP = true;

   private List<Object> keys = new ArrayList<Object>(MAX_OVERALL_KEYS);
   protected static boolean USE_TRANSACTIONS = false;

   public static void main(String[] args) throws Exception {
      ProfileTest pst = new ProfileTest();
      pst.startedInCmdLine = true;

      String mode = args[0];
      if (args.length > 1) USE_TRANSACTIONS = Boolean.parseBoolean(args[1]);

      try {
         if (args.length > 1) pst.clusterNameOverride = args[1];
         pst.testWith(mode);
      } finally {
         pst.destroyAfterMethod();
         pst.destroyAfterClass();
      }
   }

   protected void testWith(String cacheName) throws Exception {
      log.warnf("Starting profile test, cache name = %s", cacheName);
      initTest();
      cache = cacheManager.getCache(cacheName);
      runCompleteTest(cacheName);
   }


   @Test(enabled = false)
   public void testLocalMode() throws Exception {
      runCompleteTest(LOCAL_CACHE_NAME);
   }

   @Test(enabled = false)
   public void testReplMode() throws Exception {
      runCompleteTest(REPL_SYNC_CACHE_NAME);
   }

   private void runCompleteTest(String cacheName) throws Exception {
      cache = cacheManager.getCache(cacheName);
      init();
      startup();
      if (!cacheName.equals(LOCAL_CACHE_NAME)) {
         System.out.println("Waiting for members to join.");
         TestingUtil.blockUntilViewReceived(cache, 2, 120000, true);
         System.out.println("Cluster ready, cache mode is " + cache.getCacheConfiguration().clustering().cacheMode());
      }
      warmup();
      doTest();
   }

   /**
    * Thr following test phases can be profiled individually using triggers in JProfiler.
    */

   protected void init() {
      long startTime = System.currentTimeMillis();
      log.warn("Starting init() phase");
      keys.clear();
      for (int i = 0; i < MAX_OVERALL_KEYS; i++) {
         Object key;
         do {
            key = Generator.createRandomKey();
         }
         while (keys.contains(key));

         if (i % 10 == 0) {
            log.trace("Generated " + i + " keys");
         }
         keys.add(key);
      }
      System.gc();
      long duration = System.currentTimeMillis() - startTime;
      log.warn("Finished init() phase.  " + printDuration(duration));
   }


   protected void startup() {
      long startTime = System.currentTimeMillis();
      log.warn("Starting cache");
      cache.start();
      long duration = System.currentTimeMillis() - startTime;
      log.warn("Started cache.  " + printDuration(duration));
   }

   private void warmup() throws InterruptedException {
      if (SKIP_WARMUP) {
         log.info("Skipping warmup; sleeping 3 secs");
         TestingUtil.sleepThread(3000);
         return;
      }
      long startTime = System.currentTimeMillis();
      TaskRunner exec = new TaskRunner(NUM_THREADS, true);
      log.warn("Starting warmup");
      for (final Object key : keys) {
         exec.execute(new Runnable() {
            @Override
            public void run() {
               // this will create the necessary entries.
               cache.put(key, Collections.emptyMap());
            }
         });
      }

      // loop through WARMUP_LOOPS gets and puts for JVM optimisation
      for (int i = 0; i < WARMUP_LOOPS; i++) {
         exec.execute(new Runnable() {
            @Override
            public void run() {
               Object key = Generator.getRandomElement(keys);
               cache.get(key);
               cache.put(key, "Value");
               cache.remove(key);
            }
         });
      }

      exec.stop();

      long duration = System.currentTimeMillis() - startTime;
      log.warn("Finished warmup.  " + printDuration(duration));
      cache.stop();

      startup();
   }

   private void doTest() throws Exception {
      TaskRunner exec = new TaskRunner(NUM_THREADS);
      log.warn("Starting test");
      int i;
      long print = NUM_OPERATIONS / 10;

      AtomicLong durationPuts = new AtomicLong();
      AtomicLong durationGets = new AtomicLong();
      AtomicLong durationRemoves = new AtomicLong();

      long stElapsed = System.nanoTime();
      for (i = 0; i < NUM_OPERATIONS; i++) {
         MyRunnable r = null;
         switch (i % 3) {
            case 0:
               r = new Putter(i, durationPuts);
               break;
            case 1:
               r = new Getter(i, durationGets);
               break;
            case 2:
               r = new Remover(i, durationRemoves);
               break;
         }
         if (i % print == 0)
            log.warn("processing iteration " + i);
         exec.execute(r);
//         if (USE_SLEEP) TestingUtil.sleepRandom(MAX_RANDOM_SLEEP_MILLIS);
         if (USE_SLEEP) TestingUtil.sleepThread(MAX_RANDOM_SLEEP_MILLIS);
      }
      log.warn("Finished generating runnables; awaiting executor completion");
      // wait for executors to complete!
      exec.stop();

      // wait up to 1 sec for each call?
      long elapsedTimeNanos = System.nanoTime() - stElapsed;

      log.warn("Finished test.  " + printDuration((long) toMillis(elapsedTimeNanos)));
      log.warn("Throughput: " + ((double) NUM_OPERATIONS * 1000 / toMillis(elapsedTimeNanos)) + " operations per second (roughly equal numbers of PUT, GET and REMOVE)");
      log.warn("Average GET time: " + printAvg(durationGets.get()));
      log.warn("Average PUT time: " + printAvg(durationPuts.get()));
      log.warn("Average REMOVE time: " + printAvg(durationRemoves.get()));
   }

   private String printAvg(long totalNanos) {
      double nOps = NUM_OPERATIONS / 3;
      double avg = (totalNanos) / nOps;
      double avgMicros = avg / 1000;
      return avgMicros + " µs";
   }

   private double toMillis(long nanos) {
      return ((double) nanos / (double) 1000000);
   }

   enum Mode {
      PUT, GET, REMOVE
   }

   private abstract class MyRunnable implements Runnable {
      int id;
      Mode mode;
      AtomicLong duration;

      @Override
      public void run() {
         try {
            Object key = Generator.getRandomElement(keys);
            long d = 0, st = 0;
            switch (mode) {
               case PUT:
                  Object value = Generator.getRandomString();
                  st = System.nanoTime();
                  if (USE_TRANSACTIONS) TestingUtil.getTransactionManager(cache).begin();
                  cache.put(key, value);
                  if (USE_TRANSACTIONS) TestingUtil.getTransactionManager(cache).commit();
                  d = System.nanoTime() - st;
                  break;
               case GET:
                  st = System.nanoTime();
                  if (USE_TRANSACTIONS) TestingUtil.getTransactionManager(cache).begin();
                  cache.get(key);
                  if (USE_TRANSACTIONS) TestingUtil.getTransactionManager(cache).commit();
                  d = System.nanoTime() - st;
                  break;
               case REMOVE:
                  st = System.nanoTime();
                  if (USE_TRANSACTIONS) TestingUtil.getTransactionManager(cache).begin();
                  cache.remove(key);
                  if (USE_TRANSACTIONS) TestingUtil.getTransactionManager(cache).commit();
                  d = System.nanoTime() - st;
                  break;
            }
            duration.getAndAdd(d);
         } catch (Exception e) {
            log.error("Caught ", e);
         }
      }
   }

   private class Putter extends MyRunnable {
      private Putter(int id, AtomicLong duration) {
         this.id = id;
         this.duration = duration;
         mode = Mode.PUT;
      }
   }

   private class Getter extends MyRunnable {
      private Getter(int id, AtomicLong duration) {
         this.id = id;
         this.duration = duration;
         mode = Mode.GET;
      }
   }

   private class Remover extends MyRunnable {
      private Remover(int id, AtomicLong duration) {
         this.id = id;
         this.duration = duration;
         mode = Mode.REMOVE;
      }
   }

   protected String printDuration(long duration) {
      if (duration > 2000) {
         double dSecs = ((double) duration / (double) 1000);
         return "Duration: " + dSecs + " seconds";
      } else {
         return "Duration: " + duration + " millis";
      }
   }
}
