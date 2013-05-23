/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.container.*;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Random;
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
   volatile CountDownLatch latch;
   final int RUN_TIME_MILLIS = 45 * 1000; // 1 min
   final int WARMUP_TIME_MILLIS = 10 * 1000; // 10 sec
   final int num_loops = 10000;
   final int warmup_num_loops = 10000;
   boolean use_time = true;
   final int NUM_KEYS = 100;

   private static final Log log = LogFactory.getLog(DataContainerStressTest.class);
   private static final Random R = new Random();

   public void testSimpleDataContainer() throws InterruptedException {
      doTest(DefaultDataContainer.unBoundedDataContainer(5000));
   }

   private void doTest(final DataContainer dc) throws InterruptedException {
      doTest(dc, true);
      doTest(dc, false);
   }

   private void doTest(final DataContainer dc, boolean warmup) throws InterruptedException {
      latch = new CountDownLatch(1);
      final String key = "key";
      final Map<String, String> perf = new ConcurrentSkipListMap<String, String>();
      final AtomicBoolean run = new AtomicBoolean(true);
      final int actual_num_loops = warmup ? warmup_num_loops : num_loops;

      Thread getter = new Thread() {
         public void run() {
            waitForStart();
            long start = System.nanoTime();
            int runs = 0;
            while (use_time && run.get() || runs < actual_num_loops) {
               if (runs % 100000 == 0) log.info("GET run # " + runs);
//               TestingUtil.sleepThread(10);
               dc.get(key + R.nextInt(NUM_KEYS));
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
            while (use_time && run.get() || runs < actual_num_loops) {
               if (runs % 100000 == 0) log.info("PUT run # " + runs);
//               TestingUtil.sleepThread(10);
               dc.put(key + R.nextInt(NUM_KEYS), "value", new EmbeddedMetadata.Builder().build());
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
            while (use_time && run.get() || runs < actual_num_loops) {
               if (runs % 100000 == 0) log.info("REM run # " + runs);
//               TestingUtil.sleepThread(10);
               dc.remove(key + R.nextInt(NUM_KEYS));
               runs++;
            }
            perf.put("REM", opsPerMS(System.nanoTime() - start, runs));
         }
      };

      Thread[] threads = {getter, putter, remover};
      for (Thread t : threads) t.start();
      latch.countDown();

      // wait some time
      Thread.sleep(warmup ? WARMUP_TIME_MILLIS : RUN_TIME_MILLIS);
      run.set(false);
      for (Thread t : threads) t.join();
      if (!warmup) log.warnf("%s: Performance: %s", dc.getClass().getSimpleName(), perf);
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
