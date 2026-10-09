package org.infinispan.commons.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.hash.MurmurHash3Old;
import org.junit.jupiter.api.Test;

public class BloomFilterTest {

   @Test
   public void testBasicBloomFilter() {
      BloomFilter<byte[]> filter = MurmurHash3BloomFilter.createFilter(1024);
      byte[] item1 = new byte[]{1, 2, 3, 4};
      byte[] item2 = new byte[]{5, 6, 7, 8};

      assertTrue(filter.addToFilter(item1));
      assertTrue(filter.addToFilter(item2));

      assertTrue(filter.possiblyPresent(item1));
      assertTrue(filter.possiblyPresent(item2));
   }

   @Test
   public void testSetBitsByteArrayAndIntSet() {
      BloomFilter<byte[]> source = MurmurHash3BloomFilter.createFilter(4000);
      Random random = new Random(42);
      for (int i = 0; i < 500; i++) {
         byte[] key = new byte[16];
         random.nextBytes(key);
         source.addToFilter(key);
      }

      byte[] bitSetBytes = source.getIntSet().toBitSet();

      // Test setBits(byte[]) on concurrent filter
      BloomFilter<byte[]> concurrentFilter = MurmurHash3BloomFilter.createConcurrentFilter(4000);
      concurrentFilter.setBits(bitSetBytes);

      assertEquals(source.getIntSet().size(), concurrentFilter.getIntSet().size());
      assertEquals(source.getIntSet(), concurrentFilter.getIntSet());

      // Test setBits(IntSet) on concurrent filter
      BloomFilter<byte[]> concurrentFilter2 = MurmurHash3BloomFilter.createConcurrentFilter(4000);
      concurrentFilter2.setBits(source.getIntSet());

      assertEquals(source.getIntSet().size(), concurrentFilter2.getIntSet().size());
      assertEquals(source.getIntSet(), concurrentFilter2.getIntSet());
   }

   @Test
   public void testConcurrentSmallIntSetWordOperations() {
      ConcurrentSmallIntSet cset = new ConcurrentSmallIntSet(4000);
      SmallIntSet sset = new SmallIntSet();

      // Populate sset with random bits
      Random random = new Random(123);
      for (int i = 0; i < 200; i++) {
         sset.add(random.nextInt(3900));
      }

      // Test addAll(SmallIntSet)
      assertTrue(cset.addAll(sset));
      assertEquals(sset.size(), cset.size());
      assertEquals(sset, cset);

      // Test addAll idempotent
      assertFalse(cset.addAll(sset));

      // Test setBits(SmallIntSet)
      ConcurrentSmallIntSet cset2 = new ConcurrentSmallIntSet(4000);
      cset2.setBits(sset);
      assertEquals(sset.size(), cset2.size());
      assertEquals(sset, cset2);

      // Test setBits(ConcurrentSmallIntSet)
      ConcurrentSmallIntSet cset3 = new ConcurrentSmallIntSet(4000);
      cset3.setBits(cset2);
      assertEquals(cset2.size(), cset3.size());
      assertEquals(cset2, cset3);

      // Test setBits(byte[])
      byte[] bytes = sset.toBitSet();
      ConcurrentSmallIntSet cset4 = new ConcurrentSmallIntSet(4000);
      cset4.setBits(bytes);
      assertEquals(sset.size(), cset4.size());
      assertEquals(sset, cset4);
   }

   @Test
   public void testAllocationAndThroughputMicrobenchmark() {
      byte[] key = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
      int warmup = 50_000;
      int iterations = 1_000_000;

      for (int i = 0; i < warmup; i++) {
         MurmurHash3Old.MurmurHash3_x64_32(key, 239);
         MurmurHash3.MurmurHash3_x64_32(key, 239);
      }

      long startOld = System.nanoTime();
      int sumOld = 0;
      for (int i = 0; i < iterations; i++) {
         sumOld += MurmurHash3Old.MurmurHash3_x64_32(key, 239);
      }
      long durOld = System.nanoTime() - startOld;

      long startNew = System.nanoTime();
      int sumNew = 0;
      for (int i = 0; i < iterations; i++) {
         sumNew += MurmurHash3.MurmurHash3_x64_32(key, 239);
      }
      long durNew = System.nanoTime() - startNew;

      assertEquals(sumOld, sumNew);
      System.out.println("MurmurHash3Old (allocates State): " + (durOld / 1_000_000.0) + " ms (" + ((double) durOld / iterations) + " ns/op)");
      System.out.println("MurmurHash3 (allocation-free):   " + (durNew / 1_000_000.0) + " ms (" + ((double) durNew / iterations) + " ns/op)");
      System.out.println("Speedup: " + String.format("%.2fx", (double) durOld / durNew));
   }

   @Test
   public void testBloomFilterAddToFilterThroughput() {
      BloomFilter<byte[]> filter = MurmurHash3BloomFilter.createFilter(4000);
      byte[] key = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
      int warmup = 50_000;
      int iterations = 1_000_000;

      for (int i = 0; i < warmup; i++) {
         filter.addToFilter(key);
      }

      long start = System.nanoTime();
      for (int i = 0; i < iterations; i++) {
         filter.addToFilter(key);
      }
      long dur = System.nanoTime() - start;
      System.out.println("BloomFilter.addToFilter: " + (dur / 1_000_000.0) + " ms (" + ((double) dur / iterations) + " ns/op)");
   }

   @Test
   public void testMicrobenchmarkServerSetBits() {
      int bitsToUse = 4000;
      BloomFilter<byte[]> clientFilter = MurmurHash3BloomFilter.createFilter(bitsToUse);
      Random random = new Random(42);
      for (int i = 0; i < 500; i++) {
         byte[] k = new byte[16];
         random.nextBytes(k);
         clientFilter.addToFilter(k);
      }
      byte[] bloomBytes = clientFilter.getIntSet().toBitSet();

      BloomFilter<byte[]> serverFilter = MurmurHash3BloomFilter.createConcurrentFilter(bitsToUse);
      int warmup = 20_000;
      int iterations = 100_000;

      for (int i = 0; i < warmup; i++) {
         serverFilter.setBits(bloomBytes);
      }

      long start = System.nanoTime();
      for (int i = 0; i < iterations; i++) {
         serverFilter.setBits(bloomBytes);
      }
      long dur = System.nanoTime() - start;
      System.out.println("Optimized serverFilter.setBits(byte[]): " + (dur / 1_000_000.0) + " ms (" + ((double) dur / iterations) + " ns/op)");
   }

   @Test
   public void testConcurrentSetBitsAndAddSizeConsistency() throws Exception {
      ConcurrentSmallIntSet set = new ConcurrentSmallIntSet(1024);
      AtomicBoolean stop = new AtomicBoolean(false);
      int numWriters = 4;
      ExecutorService executor = Executors.newFixedThreadPool(numWriters + 1);

      // Thread 1: calls setBits repeatedly with random bitsets
      Future<?> setBitsTask = executor.submit(() -> {
         Random random = new Random(42);
         byte[] bytes = new byte[128]; // 1024 bits
         while (!stop.get()) {
            random.nextBytes(bytes);
            set.setBits(bytes);
         }
      });

      // Threads 2-5: concurrently call add and remove on random bits
      List<Future<?>> tasks = new ArrayList<>();
      for (int t = 0; t < numWriters; t++) {
         final int threadId = t;
         tasks.add(executor.submit(() -> {
            Random random = new Random(100 + threadId);
            while (!stop.get()) {
               int bit = random.nextInt(1000);
               if (random.nextBoolean()) {
                  set.add(bit);
               } else {
                  set.remove(bit);
               }
            }
         }));
      }

      Thread.sleep(500);
      stop.set(true);

      setBitsTask.get();
      for (Future<?> task : tasks) {
         task.get();
      }
      executor.shutdown();

      // Verify that currentSize matches the exact count of set bits in the set
      int actualBits = 0;
      for (int i = 0; i < 1024; i++) {
         if (set.contains(i)) {
            actualBits++;
         }
      }
      assertEquals(actualBits, set.size(), "Size was desynchronized under concurrent updates!");
   }
}
