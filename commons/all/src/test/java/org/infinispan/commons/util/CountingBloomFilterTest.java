package org.infinispan.commons.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToIntFunction;

import org.junit.jupiter.api.Test;

public class CountingBloomFilterTest {
   private static final int BITS = 64;

   /**
    * Every value is hashed to its own value, which makes the bits a value occupies trivial to reason about.
    */
   private static CountingBloomFilter<Integer> identityFilter(int bits, int... offsets) {
      @SuppressWarnings("unchecked")
      ToIntFunction<? super Integer>[] functions = new ToIntFunction[offsets.length];
      for (int i = 0; i < offsets.length; ++i) {
         int offset = offsets[i];
         functions[i] = value -> value + offset;
      }
      return CountingBloomFilter.createFilter(bits, functions);
   }

   private static byte[] key(int value) {
      return ("key-" + value).getBytes(StandardCharsets.UTF_8);
   }

   @Test
   public void testAddAndRemoveSingleValue() {
      CountingBloomFilter<Integer> filter = identityFilter(BITS, 0, 1, 2);

      assertFalse(filter.possiblyPresent(1));
      assertTrue(filter.addToFilter(1));
      assertTrue(filter.possiblyPresent(1));
      assertArrayEquals(new int[]{1, 2, 3}, filter.getIntSet().toIntArray());

      assertEquals(3, filter.removeFromFilter(1));
      assertFalse(filter.possiblyPresent(1));
      assertTrue(filter.getIntSet().isEmpty());
   }

   @Test
   public void testAddingSameValueTwiceDoesNotSetNewBits() {
      CountingBloomFilter<Integer> filter = identityFilter(BITS, 0, 1, 2);

      assertTrue(filter.addToFilter(1));
      assertFalse(filter.addToFilter(1));

      // The first removal only undoes the second addition
      assertEquals(0, filter.removeFromFilter(1));
      assertTrue(filter.possiblyPresent(1));

      assertEquals(3, filter.removeFromFilter(1));
      assertFalse(filter.possiblyPresent(1));
   }

   @Test
   public void testRemoveOnlyClearsBitsNoOtherValueUses() {
      CountingBloomFilter<Integer> filter = identityFilter(BITS, 0, 1, 2);

      filter.addToFilter(1);
      // Bits 2, 3 and 4, so it overlaps on bits 2 and 3
      filter.addToFilter(2);

      assertEquals(1, filter.removeFromFilter(1));
      assertFalse(filter.possiblyPresent(1));
      assertTrue(filter.possiblyPresent(2));
      assertArrayEquals(new int[]{2, 3, 4}, filter.getIntSet().toIntArray());

      assertEquals(3, filter.removeFromFilter(2));
      assertTrue(filter.getIntSet().isEmpty());
   }

   @Test
   public void testCountersNeverGoBelowZero() {
      CountingBloomFilter<Integer> filter = identityFilter(BITS, 0, 1, 2);

      // Removing from an empty filter is a no op, the counters must not underflow
      assertEquals(0, filter.removeFromFilter(1));
      assertEquals(0, filter.removeFromFilter(1));
      assertTrue(filter.getIntSet().isEmpty());

      assertTrue(filter.addToFilter(1));
      assertArrayEquals(new int[]{1, 2, 3}, filter.getIntSet().toIntArray());
      assertEquals(3, filter.removeFromFilter(1));
      assertTrue(filter.getIntSet().isEmpty());
   }

   @Test
   public void testCountersSaturate() {
      CountingBloomFilter<Integer> filter = identityFilter(BITS, 0);

      for (int i = 0; i < CountingBloomFilter.MAX_COUNT; ++i) {
         assertEquals(i == 0, filter.addToFilter(1));
      }
      // The counter is saturated, additional additions do not change it
      assertFalse(filter.addToFilter(1));

      // A saturated counter is never decremented again, so the bit stays set and no false negative is possible
      for (int i = 0; i < CountingBloomFilter.MAX_COUNT + 1; ++i) {
         assertEquals(0, filter.removeFromFilter(1));
      }
      assertTrue(filter.possiblyPresent(1));
   }

   @Test
   public void testClear() {
      CountingBloomFilter<Integer> filter = identityFilter(BITS, 0, 1, 2);

      for (int i = 0; i < 10; ++i) {
         filter.addToFilter(i);
      }
      assertFalse(filter.getIntSet().isEmpty());

      filter.clear();

      assertTrue(filter.getIntSet().isEmpty());
      assertArrayEquals(new byte[0], filter.toBitSet());
      for (int i = 0; i < 10; ++i) {
         assertFalse(filter.possiblyPresent(i));
      }
   }

   @Test
   public void testBitsWrapAroundFilterSize() {
      CountingBloomFilter<Integer> filter = identityFilter(8, 0, 8);

      assertTrue(filter.addToFilter(3));
      // Both hash functions map to the same bit
      assertArrayEquals(new int[]{3}, filter.getIntSet().toIntArray());
      assertEquals(1, filter.removeFromFilter(3));
      assertTrue(filter.getIntSet().isEmpty());
   }

   @Test
   public void testMatchesPlainBloomFilter() {
      int bits = 256;
      BloomFilter<byte[]> bloomFilter = MurmurHash3BloomFilter.createFilter(bits);
      CountingBloomFilter<byte[]> countingBloomFilter = MurmurHash3CountingBloomFilter.createFilter(bits);

      for (int i = 0; i < 20; ++i) {
         assertEquals(bloomFilter.addToFilter(key(i)), countingBloomFilter.addToFilter(key(i)));
         assertArrayEquals(bloomFilter.getIntSet().toBitSet(), countingBloomFilter.toBitSet());
      }

      for (int i = 0; i < 100; ++i) {
         assertEquals(bloomFilter.possiblyPresent(key(i)), countingBloomFilter.possiblyPresent(key(i)));
      }

      // Removing every value from the counting filter has to clear every bit the plain filter had set
      int setBits = bloomFilter.getIntSet().size();
      int clearedBits = 0;
      for (int i = 0; i < 20; ++i) {
         clearedBits += countingBloomFilter.removeFromFilter(key(i));
      }
      assertEquals(setBits, clearedBits);
      assertArrayEquals(new byte[0], countingBloomFilter.toBitSet());
   }

   @Test
   public void testMatchesPlainBloomFilterAfterRemovals() {
      int bits = 256;
      CountingBloomFilter<byte[]> countingBloomFilter = MurmurHash3CountingBloomFilter.createFilter(bits);
      for (int i = 0; i < 20; ++i) {
         countingBloomFilter.addToFilter(key(i));
      }
      for (int i = 0; i < 20; i += 2) {
         countingBloomFilter.removeFromFilter(key(i));
      }

      // A plain filter built from the values that are left has to hold the very same bits
      BloomFilter<byte[]> bloomFilter = MurmurHash3BloomFilter.createFilter(bits);
      for (int i = 1; i < 20; i += 2) {
         bloomFilter.addToFilter(key(i));
      }

      assertArrayEquals(bloomFilter.getIntSet().toBitSet(), countingBloomFilter.toBitSet());
   }

   @Test
   public void testInvalidArguments() {
      assertThrows(IllegalArgumentException.class, () -> MurmurHash3CountingBloomFilter.createFilter(0));
      assertThrows(IllegalArgumentException.class, () -> MurmurHash3CountingBloomFilter.createFilter(-1));
      assertThrows(IllegalArgumentException.class, () -> MurmurHash3CountingBloomFilter.createFilter(16, 0));
      assertThrows(IllegalArgumentException.class, () -> MurmurHash3CountingBloomFilter.createFilter(16, 11));
   }

   @Test
   public void testConcurrentAddAndRemove() throws Exception {
      int threads = 8;
      int iterations = 2_000;
      CountingBloomFilter<byte[]> filter = MurmurHash3CountingBloomFilter.createFilter(64);
      // Few bits on purpose, so that all the threads keep fighting over the same counters
      AtomicInteger clearedBits = new AtomicInteger();
      CyclicBarrier barrier = new CyclicBarrier(threads);

      ExecutorService executorService = Executors.newFixedThreadPool(threads);
      try {
         Future<?>[] futures = new Future<?>[threads];
         for (int i = 0; i < threads; ++i) {
            int threadOffset = i * iterations;
            futures[i] = executorService.submit(() -> {
               barrier.await(10, TimeUnit.SECONDS);
               for (int j = 0; j < iterations; ++j) {
                  byte[] key = key(threadOffset + j);
                  filter.addToFilter(key);
                  assertTrue(filter.possiblyPresent(key));
                  clearedBits.addAndGet(filter.removeFromFilter(key));
               }
               return null;
            });
         }
         for (Future<?> future : futures) {
            future.get(1, TimeUnit.MINUTES);
         }
      } finally {
         executorService.shutdownNow();
      }

      // Every bit that was ever set was also cleared again, the filter is back to being empty
      assertTrue(filter.getIntSet().isEmpty(), () -> "Filter was not empty: " + filter);
      assertTrue(clearedBits.get() > 0);
   }
}
