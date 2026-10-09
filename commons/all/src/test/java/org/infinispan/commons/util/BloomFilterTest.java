package org.infinispan.commons.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;

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
   public void testIntegerMinValue() {
      // Test that Integer.MIN_VALUE does not throw IndexOutOfBoundsException
      BloomFilter<String> filter = BloomFilter.createFilter(1000, Collections.singletonList(s -> Integer.MIN_VALUE));
      filter.addToFilter("test");
      assertTrue(filter.possiblyPresent("test"));
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
}
