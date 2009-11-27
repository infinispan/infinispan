package org.infinispan.stress;

import org.infinispan.container.DataContainer;
import org.infinispan.container.FIFOAMRDataContainer;
import org.infinispan.container.FIFODataContainer;
import org.infinispan.container.LRUAMRDataContainer;
import org.infinispan.container.LRUDataContainer;
import org.infinispan.container.SimpleDataContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Stress test different data containers
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Test(testName = "stress.DataContainerStressTest", groups = "stress", enabled = false,
      description = "Disabled by default, designed to be run manually.")
public class DataContainerStressTest {
   final CountDownLatch latch = new CountDownLatch(1);
   final int RUN_TIME_MILLIS = 60 * 1000; // 1 min
   final int num_loops = 10000;
   boolean use_time = true;
   private static final Log log = LogFactory.getLog(DataContainerStressTest.class);

   public void testSimpleDataContainer() throws InterruptedException {
      doTest(new SimpleDataContainer(5000));
   }

   public void testFIFODataContainer() throws InterruptedException {
      doTest(new FIFODataContainer(5000));
   }

   public void testFIFOAMRDataContainer() throws InterruptedException {
      doTest(new FIFOAMRDataContainer(5000));
   }

   public void testLRUAMRDataContainer() throws InterruptedException {
      doTest(new LRUAMRDataContainer(5000));
   }

   public void testLRUDataContainer() throws InterruptedException {
      doTest(new LRUDataContainer(5000));
   }

   private void doTest(final DataContainer dc) throws InterruptedException {
      final String key = "key";
      final Map<String, String> perf = new ConcurrentSkipListMap<String, String>();
      final AtomicBoolean run = new AtomicBoolean(true);

      Thread getter = new Thread() {
         public void run() {
            waitForStart();
            long start = System.nanoTime();
            int runs = 0;
            while (use_time && run.get() || runs < num_loops) {
               if (runs % 100000 == 0) log.info("GET run # " + runs);
//               TestingUtil.sleepThread(10);
               dc.get(key);
               runs++;
            }
            perf.put("GET", opsPerMS(System.nanoTime() - start, runs));
         }
      };

      Thread putter = new Thread() {
         public void run() {
            waitForStart();
            long start = System.nanoTime();
            int runs = 0;
            while (use_time && run.get() || runs < num_loops) {
               if (runs % 100000 == 0) log.info("PUT run # " + runs);
//               TestingUtil.sleepThread(10);
               dc.put(key, "value", -1, -1);
               runs++;
            }
            perf.put("PUT", opsPerMS(System.nanoTime() - start, runs));
         }
      };

      Thread remover = new Thread() {
         public void run() {
            waitForStart();
            long start = System.nanoTime();
            int runs = 0;
            while (use_time && run.get() || runs < num_loops) {
               if (runs % 100000 == 0) log.info("REM run # " + runs);
//               TestingUtil.sleepThread(10);
               dc.remove(key);
               runs++;
            }
            perf.put("REM", opsPerMS(System.nanoTime() - start, runs));
         }
      };

      Thread[] threads = {getter, putter, remover};
      for (Thread t : threads) t.start();
      latch.countDown();

      // wait some time
      Thread.sleep(RUN_TIME_MILLIS);
      run.set(false);
      for (Thread t : threads) t.join();
      log.warn("{0}: Performance: {1}", dc.getClass().getSimpleName(), perf);
   }

   private void waitForStart() {
      try {
         latch.await();
      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }

   private String opsPerMS(long nanos, int ops) {
      long totalMillis = TimeUnit.NANOSECONDS.toMillis(nanos);
      if (totalMillis > 0)
         return ops / totalMillis + " ops/ms";
      else
         return "NAN ops/ms";
   }
}
