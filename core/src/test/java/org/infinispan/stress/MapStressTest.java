/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ComparingObject;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.*;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static java.lang.Math.sqrt;

/**
 * Stress test different maps for container implementations
 *
 * @author Manik Surtani
 * @author Dan Berindei <dberinde@redhat.com>
 * @since 4.0
 */
@Test(testName = "stress.MapStressTest", groups = "stress", enabled = false, description = "Disabled by default, designed to be run manually.")
public class MapStressTest {
   private static Log log = LogFactory.getLog(MapStressTest.class);

   static final float MAP_LOAD_FACTOR = 0.75f;
   static final int LOOP_FACTOR = 10;
   static final long RUNNING_TIME = Integer.getInteger("time", 1) * 60 * 1000;
   final int CAPACITY = Integer.getInteger("size", 100000);

   private static final Random RANDOM = new Random(12345);

   private volatile CountDownLatch latch;
   private List<String> keys = new ArrayList<String>();

   public MapStressTest() {
      log.tracef("\nMapStressTest configuration: capacity %d, test running time %d seconds\n",
                        CAPACITY, RUNNING_TIME / 1000);
   }


   private void generateKeyList(int numKeys) {
      // without this we keep getting OutOfMemoryErrors
      keys = null;
      keys = new ArrayList<String>(numKeys * LOOP_FACTOR);
      for (int i = 0; i < numKeys * LOOP_FACTOR; i++) {
         keys.add("key" + nextIntGaussian(numKeys));
      }
   }

   private int nextIntGaussian(int numKeys) {
      double gaussian = RANDOM.nextGaussian();
      if (gaussian < -3 || gaussian > 3)
         return nextIntGaussian(numKeys);

      return (int) Math.abs((gaussian + 3) * numKeys / 6);
   }

   private Map<String, Integer> synchronizedLinkedHashMap(final int capacity, float loadFactor) {
      return Collections.synchronizedMap(new LinkedHashMap<String, Integer>(capacity, loadFactor, true) {
         @Override
         protected boolean removeEldestEntry(Entry<String, Integer> eldest) {
            return size() > capacity;
         }
      });
   }

   private Cache<String, Integer> configureAndBuildCache(int capacity) {
      Configuration config = new Configuration().fluent()
         .eviction().maxEntries(capacity).strategy(EvictionStrategy.LRU)
         .expiration().wakeUpInterval(5000L).maxIdle(120000L)
         .build();

      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(
            GlobalConfiguration.getNonClusteredDefault(), config);
      cm.start();
      return cm.getCache();
   }

   @DataProvider(name = "readWriteRemove")
   public Object[][] independentReadWriteRemoveParams() {
      return new Object[][]{
            new Object[]{CAPACITY, 3 * CAPACITY, 32, 90, 9, 1},
            new Object[]{CAPACITY, 3 * CAPACITY, 32, 9, 1, 0},
      };
   }

   @DataProvider(name = "readWriteRatio")
   public Object[][] readWriteRatioParams() {
      return new Object[][]{
            new Object[]{CAPACITY, 3 * CAPACITY, 32, 100, 9},
            new Object[]{CAPACITY, 3 * CAPACITY, 32, 10, 9},
      };
   }

   @DataProvider(name = "writeOnMiss")
   public Object[][] writeOnMissParams() {
      return new Object[][]{
            new Object[]{CAPACITY, 3 * CAPACITY, 32, 100},
            new Object[]{CAPACITY, 3 * CAPACITY, 32, 10},
      };
   }

   private Map<String, Map<String, Integer>> createMaps(int capacity, int numKeys, int concurrency) {
      Map<String, Map<String, Integer>> maps = new TreeMap<String, Map<String, Integer>>();
      maps.put("BCHM:LRU", new BoundedConcurrentHashMap<String, Integer>(
            capacity, concurrency, BoundedConcurrentHashMap.Eviction.LRU,
            ComparingObject.INSTANCE, ComparingObject.INSTANCE));
      maps.put("BCHM:LIRS", new BoundedConcurrentHashMap<String, Integer>(
            capacity, concurrency, BoundedConcurrentHashMap.Eviction.LIRS,
            ComparingObject.INSTANCE, ComparingObject.INSTANCE));
      // CHM doesn't have eviction, so we size it to the total number of keys to avoid resizing
      maps.put("CHM", new ConcurrentHashMap<String, Integer>(numKeys, MAP_LOAD_FACTOR, concurrency));
      maps.put("SLHM", synchronizedLinkedHashMap(capacity, MAP_LOAD_FACTOR));
      maps.put("CACHE", configureAndBuildCache(capacity));
      return maps;
   }

   @Test(dataProvider = "readWriteRemove", enabled = false)
   public void testReadWriteRemove(int capacity, int numKeys, int concurrency, int readerThreads, int writerThreads, int removerThreads) throws Exception {
      System.out.printf("Testing independent read/write/remove performance with capacity %d, keys %d, concurrency level %d, readers %d, writers %d, removers %d\n",
            capacity, numKeys, concurrency, readerThreads, writerThreads, removerThreads);

      generateKeyList(numKeys);
      Map<String, Map<String, Integer>> maps = createMaps(capacity, numKeys, concurrency);

      for (Entry<String, Map<String, Integer>> e : maps.entrySet()) {
         mapTestReadWriteRemove(e.getKey(), e.getValue(), numKeys, readerThreads, writerThreads, removerThreads);
         e.setValue(null);
      }
   }

   private void mapTestReadWriteRemove(String name, Map<String, Integer> map, int numKeys, int readerThreads, int writerThreads, int removerThreads) throws Exception {
      // warm up for 1 second
      runMapTestReadWriteRemove(map, readerThreads, writerThreads, removerThreads, 1000);

      // real test
      TotalStats perf = runMapTestReadWriteRemove(map, readerThreads, writerThreads, removerThreads, RUNNING_TIME);

      System.out.printf("Container %-12s  ", name);
      System.out.printf("Ops/s %10.2f  ", perf.getTotalOpsPerSec());
      System.out.printf("Gets/s %10.2f  ", perf.getOpsPerSec("GET"));
      System.out.printf("Puts/s %10.2f  ", perf.getOpsPerSec("PUT"));
      System.out.printf("Removes/s %10.2f  ", perf.getOpsPerSec("REMOVE"));
      System.out.printf("HitRatio %10.2f  ", perf.getTotalHitRatio() * 100);
      System.out.printf("Size %10d  ", map.size());
      double stdDev = computeStdDev(map, numKeys);
      System.out.printf("StdDev %10.2f\n", stdDev);
   }

   private TotalStats runMapTestReadWriteRemove(final Map<String, Integer> map, int numReaders, int numWriters,
                                                         int numRemovers, final long runningTimeout) throws Exception {
      latch = new CountDownLatch(1);
      final TotalStats perf = new TotalStats();
      List<Thread> threads = new LinkedList<Thread>();

      for (int i = 0; i < numReaders; i++) {
         Thread reader = new WorkerThread(runningTimeout, perf, readOperation(map));
         threads.add(reader);
      }

      for (int i = 0; i < numWriters; i++) {
         Thread writer = new WorkerThread(runningTimeout, perf, writeOperation(map));
         threads.add(writer);
      }

      for (int i = 0; i < numRemovers; i++) {
         Thread remover = new WorkerThread(runningTimeout, perf, removeOperation(map));
         threads.add(remover);
      }

      for (Thread t : threads)
         t.start();
      latch.countDown();

      for (Thread t : threads)
         t.join();

      return perf;
   }

   @Test(dataProvider = "readWriteRatio", enabled = false)
   public void testMixedReadWrite(int capacity, int numKeys, int concurrency, int threads, int readToWriteRatio) throws Exception {
      System.out.printf("Testing mixed read/write performance with capacity %d, keys %d, concurrency level %d, threads %d, read:write ratio %d:1\n",
            capacity, numKeys, concurrency, threads, readToWriteRatio);

      generateKeyList(numKeys);
      Map<String, Map<String, Integer>> maps = createMaps(capacity, numKeys, concurrency);

      for (Entry<String, Map<String, Integer>> e : maps.entrySet()) {
         mapTestMixedReadWrite(e.getKey(), e.getValue(), numKeys, threads, readToWriteRatio);
         e.setValue(null);
      }
   }

   private void mapTestMixedReadWrite(String name, Map<String, Integer> map, int numKeys, int threads, int readToWriteRatio) throws Exception {
      // warm up for 1 second
      runMapTestMixedReadWrite(map, threads, readToWriteRatio, 1000);

      // real test
      TotalStats perf = runMapTestMixedReadWrite(map, threads, readToWriteRatio, RUNNING_TIME);

      System.out.printf("Container %-12s  ", name);
      System.out.printf("Ops/s %10.2f  ", perf.getTotalOpsPerSec());
      System.out.printf("Gets/s %10.2f  ", perf.getTotalOpsPerSec() * readToWriteRatio / (readToWriteRatio + 1));
      System.out.printf("Puts/s %10.2f  ", perf.getTotalOpsPerSec() * 1 / (readToWriteRatio + 1));
      System.out.printf("HitRatio %10.2f  ", perf.getTotalHitRatio() * 100);
      System.out.printf("Size %10d  ", map.size());
      double stdDev = computeStdDev(map, numKeys);
      System.out.printf("stdDev %10.2f\n", stdDev);
   }

   private TotalStats runMapTestMixedReadWrite(final Map<String, Integer> map, int numThreads,
                                                   int readToWriteRatio, final long runningTimeout) throws Exception {

      latch = new CountDownLatch(1);
      final TotalStats perf = new TotalStats();
      List<Thread> threads = new LinkedList<Thread>();

      for (int i = 0; i < numThreads; i++) {
         Thread thread = new WorkerThread(runningTimeout, perf, readWriteOperation(map, readToWriteRatio));
         threads.add(thread);
      }

      for (Thread t : threads)
         t.start();
      latch.countDown();

      for (Thread t : threads)
         t.join();

      return perf;
   }

   @Test(dataProvider = "writeOnMiss", enabled = false)
   public void testWriteOnMiss(int capacity, int numKeys, int concurrency, int threads) throws Exception {
      System.out.printf("Testing write on miss performance with capacity %d, keys %d, concurrency level %d, threads %d\n",
            capacity, numKeys, concurrency, threads);

      generateKeyList(numKeys);
      Map<String, Map<String, Integer>> maps = createMaps(capacity, numKeys, concurrency);

      for (Entry<String, Map<String, Integer>> e : maps.entrySet()) {
         mapTestWriteOnMiss(e.getKey(), e.getValue(), numKeys, threads);
         e.setValue(null);
      }
   }

   private void mapTestWriteOnMiss(String name, Map<String, Integer> map, int numKeys, int threads) throws Exception {
      // warm up for 1 second
      runMapTestWriteOnMiss(map, threads, 1000);

      // real test
      TotalStats perf = runMapTestWriteOnMiss(map, threads, RUNNING_TIME);

      System.out.printf("Container %-12s  ", name);
      System.out.printf("Ops/s %10.2f  ", perf.getTotalOpsPerSec());
      System.out.printf("HitRatio %10.2f  ", perf.getTotalHitRatio() * 100);
      System.out.printf("Size %10d  ", map.size());
      double stdDev = computeStdDev(map, numKeys);
      System.out.printf("stdDev %10.2f\n", stdDev);
   }

   private TotalStats runMapTestWriteOnMiss(final Map<String, Integer> map, int numThreads,
                                                   final long runningTimeout) throws Exception {

      latch = new CountDownLatch(1);
      final TotalStats perf = new TotalStats();
      List<Thread> threads = new LinkedList<Thread>();

      for (int i = 0; i < numThreads; i++) {
         Thread thread = new WorkerThread(runningTimeout, perf, writeOnMissOperation(map));
         threads.add(thread);
      }

      for (Thread t : threads)
         t.start();
      latch.countDown();

      for (Thread t : threads)
         t.join();

      return perf;
   }

   private double computeStdDev(Map<String, Integer> map, int numKeys) {
      // The keys closest to the mean are suposed to be accessed more often
      // So we score each map by the standard deviation of the keys in the map
      // at the end of the test
      double variance = 0;
      for (String key : map.keySet()) {
         double value = Integer.parseInt(key.substring(3));
         variance += (value - numKeys / 2) * (value - numKeys / 2);
      }
      return sqrt(variance / map.size());
   }

   private void waitForStart() {
      try {
         latch.await();
      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }

   private Operation<String, Integer> readOperation(Map<String, Integer> map) {
      return new Operation<String, Integer>(map, "GET") {
         @Override
         public boolean call(String key, long run) {
            return map.get(key) != null;
         }
      };
   }

   private Operation<String, Integer> writeOperation(Map<String, Integer> map) {
      return new Operation<String, Integer>(map, "PUT") {
         @Override
         public boolean call(String key, long run) {
            return map.put(key, (int)run) != null;
         }
      };
   }

   private Operation<String, Integer> removeOperation(Map<String, Integer> map) {
      return new Operation<String, Integer>(map, "REMOVE") {
         @Override
         public boolean call(String key, long run) {
            return map.remove(key) != null;
         }
      };
   }

   private Operation<String, Integer> readWriteOperation(final Map<String, Integer> map, final int readToWriteRatio) {
      return new Operation<String, Integer>(map, "READ/WRITE:" + readToWriteRatio + "/1") {
         @Override
         public boolean call(String key, long run) {
            if (run % (readToWriteRatio + 1) == 0) {
               return map.put(key, (int)run) != null;
            } else {
               return map.get(key) != null;
            }
         }
      };
   }

   private Operation<String, Integer> writeOnMissOperation(final Map<String, Integer> map) {
      return new Operation<String, Integer>(map, "PUTMISSING") {
         @Override
         public boolean call(String key, long run) {
            boolean hit = map.get(key) != null;
            if (!hit) {
               map.put(key, (int)run);
            }
            return hit;
         }
      };
   }

   private class WorkerThread extends Thread {
      private final long runningTimeout;
      private final TotalStats perf;
      private Operation<String, Integer> op;

      public WorkerThread(long runningTimeout, TotalStats perf, Operation<String, Integer> op) {
         this.runningTimeout = runningTimeout;
         this.perf = perf;
         this.op = op;
      }

      public void run() {
         waitForStart();
         long startMilis = System.currentTimeMillis();
         long endMillis = startMilis + runningTimeout;
         int keyIndex = RANDOM.nextInt(keys.size());
         long runs = 0;
         long missCount = 0;
         while ((runs & 0x3FFF) != 0 || System.currentTimeMillis() < endMillis) {
            boolean hit = op.call(keys.get(keyIndex), runs);
            if (!hit) missCount++;
            keyIndex++;
            runs++;
            if (keyIndex >= keys.size()) {
               keyIndex = 0;
            }
         }
         perf.addStats(op.getName(), runs, System.currentTimeMillis() - startMilis, missCount);
      }
   }

   private static abstract class Operation<K, V> {
      protected final Map<K, V> map;
      protected final String name;

      public Operation(Map<K, V> map, String name) {
         this.map = map;
         this.name = name;
      }

      /**
       * @return Return true for a hit, false for a miss.
       */
      public abstract boolean call(K key, long run);

      public String getName() {
         return name;
      }
   }

   private static class TotalStats {
      private ConcurrentHashMap<String, OpStats> statsMap = new ConcurrentHashMap<String, OpStats>();

      public void addStats(String opName, long opCount, long runningTime, long missCount) {
         OpStats s = new OpStats(opName, opCount, runningTime, missCount);
         OpStats old = statsMap.putIfAbsent(opName, s);
         boolean replaced = old == null;
         while (!replaced) {
            old = statsMap.get(opName);
            s = new OpStats(old, opCount, runningTime, missCount);
            replaced = statsMap.replace(opName, old, s);
         }
      }

      public double getOpsPerSec(String opName) {
         OpStats s = statsMap.get(opName);
         if (s == null) return 0;
         return s.opCount * 1000. / s.runningTime * s.threadCount;
      }

      public double getTotalOpsPerSec() {
         long totalOpCount = 0;
         long totalRunningTime = 0;
         long totalThreadCount = 0;
         for (Map.Entry<String, OpStats> e : statsMap.entrySet()) {
            OpStats s = e.getValue();
            totalOpCount += s.opCount;
            totalRunningTime += s.runningTime;
            totalThreadCount += s.threadCount;
         }
         return totalOpCount * 1000. / totalRunningTime * totalThreadCount;
      }

      public double getHitRatio(String opName) {
         OpStats s = statsMap.get(opName);
         if (s == null) return 0;
         return 1 - 1. * s.missCount / s.opCount;
      }

      public double getTotalHitRatio() {
         long totalOpCount = 0;
         long totalMissCount = 0;
         for (Map.Entry<String, OpStats> e : statsMap.entrySet()) {
            OpStats s = e.getValue();
            totalOpCount += s.opCount;
            totalMissCount += s.missCount;
         }
         return 1 - 1. * totalMissCount / totalOpCount;
      }
   }

   private static class OpStats {
      public final String opName;
      public final int threadCount;
      public final long opCount;
      public final long runningTime;
      public final long missCount;

      private OpStats(String opName, long opCount, long runningTime, long missCount) {
         this.opName = opName;
         this.threadCount = 1;
         this.opCount = opCount;
         this.runningTime = runningTime;
         this.missCount = missCount;
      }

      private OpStats(OpStats base, long opCount, long runningTime, long missCount) {
         this.opName = base.opName;
         this.threadCount = base.threadCount + 1;
         this.opCount = base.opCount + opCount;
         this.runningTime = base.runningTime + runningTime;
         this.missCount = base.missCount + missCount;
      }
   }
}