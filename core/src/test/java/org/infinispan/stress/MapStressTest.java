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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap.Eviction;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Stress test different maps for container implementations
 * 
 * @author Manik Surtani
 * @since 4.0
 */
@Test(testName = "stress.MapStressTest", groups = "stress", enabled = false, description = "Run manually")
public class MapStressTest {
   volatile CountDownLatch latch;
   final int CONCURRENCY = 512;
   final int NUM_KEYS = 1200000; // 1.2 milion entries
   final long RUNNING_TIME = 1 * 60 * 1000; // 1 minute

   private List<Integer> readOps = new ArrayList<Integer>(NUM_KEYS);
   private List<Integer> writeOps = new ArrayList<Integer>(NUM_KEYS);
   private List<Integer> removeOps = new ArrayList<Integer>(NUM_KEYS);

   private static final Random RANDOM_READ = new Random(12345);
   private static final Random RANDOM_WRITE = new Random(34567);
   private static final Random RANDOM_REMOVE = new Random(56789);

   @BeforeClass
   @SuppressWarnings("unused")
   private void generateArraysForOps() {
      for (int i = 0; i < NUM_KEYS; i++) {
         readOps.add(RANDOM_READ.nextInt(NUM_KEYS));
         writeOps.add(RANDOM_WRITE.nextInt(NUM_KEYS));
         removeOps.add(RANDOM_REMOVE.nextInt(NUM_KEYS));
      }
   }

   @DataProvider(name = "capacities")
   public Object[][] capacities() {
      return new Object[][] { new Object[] { new Integer(131072) },
               // new Object[]{ new Integer(524288)},
               new Object[] { new Integer(1048576) } };
   }

   @Test(dataProvider = "capacities")
   public void testConcurrentHashMap(int mapCapacity) throws Exception {
      doTest(new ConcurrentHashMap<Integer, Integer>(mapCapacity, CONCURRENCY), mapCapacity);
   }

   @Test(dataProvider = "capacities")
   public void testBufferedConcurrentHashMapLRU(int mapCapacity) throws Exception {
      doTest(new BoundedConcurrentHashMap<Integer, Integer>(mapCapacity, CONCURRENCY, Eviction.LRU),
               mapCapacity);
   }

   @Test(dataProvider = "capacities")
   public void testBufferedConcurrentHashMapLRUOld(int mapCapacity) throws Exception {
      doTest(new BoundedConcurrentHashMap<Integer, Integer>(mapCapacity, CONCURRENCY,
               Eviction.LRU_OLD), mapCapacity);
   }

   @Test(dataProvider = "capacities")
   public void testBufferedConcurrentHashMapLIRS(int mapCapacity) throws Exception {
      doTest(new BoundedConcurrentHashMap<Integer, Integer>(mapCapacity, CONCURRENCY, Eviction.LIRS),
               mapCapacity);
   }

   @Test(dataProvider = "capacities")
   public void testHashMap(int mapCapacity) throws Exception {
      doTest(Collections.synchronizedMap(new HashMap<Integer, Integer>(mapCapacity)), mapCapacity);
   }

   @Test(dataProvider = "capacities")
   public void testCache(int mapCapacity) throws Exception {
      doTest(configureAndBuildCache(mapCapacity), mapCapacity);
   }

   private void doTest(final Map<Integer, Integer> map, int maxCapacity) throws Exception {
      doTest(map, 8, 2, 1, maxCapacity, RUNNING_TIME);
      doTest(map, 32, 4, 2, maxCapacity, RUNNING_TIME);
      doTest(map, 64, 8, 3, maxCapacity, RUNNING_TIME);
   }

   private Cache<Integer, Integer> configureAndBuildCache(int mapCapacity) {
      Configuration config = new Configuration().fluent().eviction().maxEntries(mapCapacity)
               .strategy(EvictionStrategy.LRU).wakeUpInterval(5000L).expiration().maxIdle(120000L)
               .locking().concurrencyLevel(CONCURRENCY).build();

      DefaultCacheManager cm = new DefaultCacheManager(
               GlobalConfiguration.getNonClusteredDefault(), config);
      cm.start();
      return cm.getCache();
   }

   private void doTest(final Map<Integer, Integer> map, int numReaders, int numWriters,
            int numRemovers, int maxCapacity, final long runningTimeout) throws Exception {

      latch = new CountDownLatch(1);
      final Map<String, String> perf = new ConcurrentSkipListMap<String, String>();
      List<Thread> threads = new LinkedList<Thread>();

      for (int i = 0; i < numReaders; i++) {
         Thread getter = new Thread() {
            public void run() {
               waitForStart();
               long startMilis = System.currentTimeMillis();
               int runs = 0;
               int totalRuns = 0;
               while ((System.currentTimeMillis() - startMilis) <= runningTimeout) {
                  map.get(readOps.get(runs));
                  runs++;
                  totalRuns++;
                  if (runs >= readOps.size()) {
                     runs = 0;
                  }
               }
               perf.put("GET" + Thread.currentThread().getId(),
                        opsPerMS(System.currentTimeMillis() - startMilis, totalRuns));
            }
         };
         threads.add(getter);
      }

      for (int i = 0; i < numWriters; i++) {
         Thread putter = new Thread() {
            public void run() {
               waitForStart();
               int runs = 0;
               int totalRuns = 0;
               long startMilis = System.currentTimeMillis();
               while ((System.currentTimeMillis() - startMilis) <= runningTimeout) {
                  map.put(writeOps.get(runs), runs);
                  runs++;
                  totalRuns++;
                  if (runs >= writeOps.size()) {
                     runs = 0;
                  }
               }
               perf.put("PUT" + Thread.currentThread().getId(),
                        opsPerMS(System.currentTimeMillis() - startMilis, totalRuns));
            }
         };
         threads.add(putter);
      }

      for (int i = 0; i < numRemovers; i++) {
         Thread remover = new Thread() {
            public void run() {
               waitForStart();
               int runs = 0;
               int totalRuns = 0;
               long startMilis = System.currentTimeMillis();
               while ((System.currentTimeMillis() - startMilis) <= runningTimeout) {
                  map.remove(removeOps.get(runs));
                  runs++;
                  totalRuns++;
                  if (runs >= removeOps.size()) {
                     runs = 0;
                  }
               }
               perf.put("REM" + Thread.currentThread().getId(),
                        opsPerMS(System.currentTimeMillis() - startMilis, totalRuns));
            }
         };
         threads.add(remover);
      }

      for (Thread t : threads)
         t.start();
      latch.countDown();

      for (Thread t : threads)
         t.join();

      int puts = 0, gets = 0, removes = 0;
      for (Entry<String, String> p : perf.entrySet()) {
         if (p.getKey().startsWith("PUT")) {
            puts += Integer.valueOf(p.getValue());
         }
         if (p.getKey().startsWith("GET")) {
            gets += Integer.valueOf(p.getValue());
         }
         if (p.getKey().startsWith("REM")) {
            removes += Integer.valueOf(p.getValue());
         }

      }
      System.out.println("Performance for container " + map.getClass().getSimpleName()
               + " max capacity is " + maxCapacity + "[numReaders,numWriters,numRemovers]=["
               + numReaders + "," + numWriters + "," + numRemovers + "]");
      System.out.println("Average get ops/ms " + (gets / numReaders));
      System.out.println("Average put ops/ms " + (puts / numWriters));
      System.out.println("Average remove ops/ms " + (removes / numRemovers));
      System.out.println("Size = " + map.size());
   }

   private void waitForStart() {
      try {
         latch.await();
      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }

   private String opsPerMS(long totalMillis, int ops) {
      if (totalMillis > 0)
         return "" + ops / totalMillis;
      else
         return "NAN ops/ms";
   }
}