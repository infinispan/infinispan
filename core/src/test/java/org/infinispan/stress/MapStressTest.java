package org.infinispan.stress;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.infinispan.util.concurrent.BufferedConcurrentHashMap;
import org.infinispan.util.concurrent.BufferedConcurrentHashMap.Eviction;
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
    
    final int RUN_TIME_MILLIS = 5 * 1000; // 10 sec
    final int NUM_KEYS = 50000;
    final int LOOP_FACTOR=5;
    
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
        doTest(new ConcurrentHashMap<Integer, Integer>(MAP_CAPACITY, MAP_LOAD_FACTOR, CONCURRENCY));
    }
   
    public void testBufferedConcurrentHashMapLRU() throws Exception {
        doTest(new BufferedConcurrentHashMap<Integer, Integer>(MAP_CAPACITY, MAP_LOAD_FACTOR, CONCURRENCY, Eviction.LRU,null));
    }
    
    public void testBufferedConcurrentHashMapLIRS() throws Exception {
        doTest(new BufferedConcurrentHashMap<Integer, Integer>(MAP_CAPACITY, MAP_LOAD_FACTOR, CONCURRENCY, Eviction.LIRS,null));
    }

    public void testHashMap() throws Exception {
        doTest(Collections.synchronizedMap(new HashMap<Integer, Integer>(MAP_CAPACITY, MAP_LOAD_FACTOR)));
    }

    private void doTest(final Map<Integer, Integer> map) throws Exception {
        doTest(map, 48, 6, 4, true);
    }

    private void doTest(final Map<Integer, Integer> map, int numReaders, int numWriters,
                    int numRemovers, boolean warmup) throws Exception {

        latch = new CountDownLatch(1);
        final Map<String, String> perf = new ConcurrentSkipListMap<String, String>();
        List<Thread> threads = new LinkedList<Thread>();

        for (int i = 0; i < numReaders; i++) {
            Thread getter = new Thread() {
                public void run() {
                    waitForStart();
                    long start = System.nanoTime();
                    int runs = 0;
                    while (runs < readOps.size()) {
                        map.get(readOps.get(runs));
                        runs++;
                    }
                    perf.put("GET" + Thread.currentThread().getId(), opsPerMS(System.nanoTime()
                                    - start, runs));
                }
            };
            threads.add(getter);
        }

        for (int i = 0; i < numWriters; i++) {
            Thread putter = new Thread() {
                public void run() {
                    waitForStart();
                    long start = System.nanoTime();
                    int runs = 0;
                    while (runs < writeOps.size()) {
                        map.put(writeOps.get(runs),runs);
                        runs++;
                    }
                    perf.put("PUT" + Thread.currentThread().getId(), opsPerMS(System.nanoTime()
                                    - start, runs));
                }
            };
            threads.add(putter);
        }

        for (int i = 0; i < numRemovers; i++) {
            Thread remover = new Thread() {
                public void run() {
                    waitForStart();
                    long start = System.nanoTime();
                    int runs = 0;
                    while (runs < removeOps.size()) {
                        map.remove(removeOps.get(runs));
                        runs++;
                    }
                    perf.put("REM" + Thread.currentThread().getId(), opsPerMS(System.nanoTime()
                                    - start, runs));
                }
            };
            threads.add(remover);
        }

        for (Thread t : threads)
            t.start();
        latch.countDown();

        // wait some time
        Thread.sleep(RUN_TIME_MILLIS);
        for (Thread t : threads)
            t.join();
        
        System.out.println("Size = " + map.size());

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
            return "" + ops / totalMillis;
        else
            return "NAN ops/ms";
    }
}