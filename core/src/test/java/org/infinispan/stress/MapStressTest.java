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
import org.testng.annotations.Test;

/**
 * Stress test different maps for container implementations
 * 
 * @author Manik Surtani
 * @since 4.0
 */
@Test(testName = "stress.MapStressTest", groups = "stress", enabled = false, description = "Disabled by default, designed to be run manually.")
public class MapStressTest {
    volatile CountDownLatch latch;
    final int MAP_CAPACITY = 512;
    final float MAP_LOAD_FACTOR = 0.75f;
    final int CONCURRENCY = 32;
    
    final int NUM_KEYS = 50 * 1000;
    final int LOOP_FACTOR = 20;
    
    final long RUNNING_TIME = 30 * 1000;
    
    private List<Integer> readOps = new ArrayList<Integer>(NUM_KEYS*LOOP_FACTOR);
    private List<Integer> writeOps = new ArrayList<Integer>(NUM_KEYS*LOOP_FACTOR);
    private List<Integer> removeOps = new ArrayList<Integer>(NUM_KEYS*LOOP_FACTOR);
    
    private static final Random RANDOM_READ = new Random(12345);
    private static final Random RANDOM_WRITE = new Random(34567);
    private static final Random RANDOM_REMOVE = new Random(56789);

    @BeforeClass
    private void generateArraysForOps() {
        for(int i = 0;i<NUM_KEYS*LOOP_FACTOR;i++) {
            readOps.add(RANDOM_READ.nextInt(NUM_KEYS));
            writeOps.add(RANDOM_WRITE.nextInt(NUM_KEYS));
            removeOps.add(RANDOM_REMOVE.nextInt(NUM_KEYS));
        }
    }
    
    public void testConcurrentHashMap() throws Exception {
        doTest(new ConcurrentHashMap<Integer, Integer>(MAP_CAPACITY, CONCURRENCY));
    }
   
    public void testBufferedConcurrentHashMapLRU() throws Exception {
        doTest(new BoundedConcurrentHashMap<Integer, Integer>(MAP_CAPACITY, CONCURRENCY, Eviction.LRU));
    }
    
    public void testBufferedConcurrentHashMapLIRS() throws Exception {
        doTest(new BoundedConcurrentHashMap<Integer, Integer>(MAP_CAPACITY, CONCURRENCY, Eviction.LIRS));
    }

    public void testHashMap() throws Exception {
        doTest(Collections.synchronizedMap(new HashMap<Integer, Integer>(MAP_CAPACITY, MAP_LOAD_FACTOR)));
    }

    private void doTest(final Map<Integer, Integer> map) throws Exception {
        //total threads = 27+3+2=32=CONCURRENCY
        doTest(map, 27, 3, 2, RUNNING_TIME);
    }
    
    public void testCache() throws Exception {
       doTest(configureAndBuildCache());
    }
    
    private Cache<Integer,Integer> configureAndBuildCache(){
       Configuration config = new Configuration().fluent().eviction().maxEntries(MAP_CAPACITY).strategy(EvictionStrategy.LRU)
         .wakeUpInterval(5000L)
         .expiration()
         .maxIdle(120000L)
         .build();
       
       DefaultCacheManager cm = new DefaultCacheManager(GlobalConfiguration.getNonClusteredDefault(),config); 
       cm.start();
       return cm.getCache();
    }

    private void doTest(final Map<Integer, Integer> map, int numReaders, int numWriters,
                    int numRemovers, final long runningTimeout) throws Exception {

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
                        totalRuns ++;
                        if(runs >= readOps.size()){
                           runs = 0;
                        }
                    }
                    perf.put("GET" + Thread.currentThread().getId(), opsPerMS(System.currentTimeMillis()
                                    - startMilis, totalRuns));
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
                        map.put(writeOps.get(runs),runs);                        
                        runs++;
                        totalRuns ++;
                        if(runs >= writeOps.size()){
                           runs = 0;
                        }                        
                    }
                    perf.put("PUT" + Thread.currentThread().getId(), opsPerMS(System.currentTimeMillis()
                                    - startMilis, totalRuns));
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
                        totalRuns ++;
                        if(runs >= removeOps.size()){
                           runs = 0;
                        }
                    }
                    perf.put("REM" + Thread.currentThread().getId(), opsPerMS(System.currentTimeMillis()
                                    - startMilis, totalRuns));
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
        System.out.println("Performance for container " + map.getClass().getSimpleName());
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