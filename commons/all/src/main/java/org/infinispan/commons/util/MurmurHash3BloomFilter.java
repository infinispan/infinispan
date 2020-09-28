package org.infinispan.commons.util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.ToIntFunction;

import org.infinispan.commons.hash.MurmurHash3;

/**
 * BloomFilter implementation that allows for up to 10 hash functions all using MurmurHash3 with different
 * seeds. The same seed(s) is used for the given number of hash functions. That is if you create this
 * filter in one JVM with 3 functions  it will be the same bloom filter as another JVM with 3 functions in another.
 * <p>
 * The default number of hash functions is 3.
 */
public class MurmurHash3BloomFilter extends BloomFilter<byte[]> {
   MurmurHash3BloomFilter(int bitsToUse, IntSet intSet, int hashFunctions) {
      super(bitsToUse, intSet, (Iterable) functions(hashFunctions));
   }

   private static int defaultHashFunctionCount() {
      return Integer.parseInt(System.getProperty("infinispan.bloom-filter.hash-functions", "3"));
   }

   public static BloomFilter<byte[]> createFilter(int bitsToUse) {
      return createFilter(bitsToUse, defaultHashFunctionCount());
   }

   public static BloomFilter<byte[]> createFilter(int bitsToUse, int hashFunctions) {
      return new MurmurHash3BloomFilter(bitsToUse, IntSets.mutableEmptySet(bitsToUse), hashFunctions);
   }

   public static BloomFilter<byte[]> createConcurrentFilter(int bitsToUse) {
      return createConcurrentFilter(bitsToUse, defaultHashFunctionCount());
   }

   public static BloomFilter<byte[]> createConcurrentFilter(int bitsToUse, int hashFunctions) {
      return new MurmurHash3BloomFilter(bitsToUse, IntSets.concurrentSet(bitsToUse), hashFunctions);
   }

   private static Iterable<ToIntFunction<byte[]>> functions(int hashFunctions) {
      if (hashFunctions <= 0) {
         throw new IllegalArgumentException("Number of hash functions must be positive, received " + hashFunctions);
      }
      List<ToIntFunction<byte[]>> functions = new ArrayList<>(hashFunctions);
      for (int i = 0; i < hashFunctions; ++i) {
         int prime = getPrime(i);
         functions.add(bytes -> MurmurHash3.MurmurHash3_x64_32(bytes, prime));
      }
      return functions;
   }

   private static int getPrime(int offset) {
      switch (offset) {
         case 0:
            return 239;
         case 1:
            return 1847;
         case 2:
            return 2719;
         case 3:
            return 3989;
         case 4:
            return 4481;
         case 5:
            return 5407;
         case 6:
            return 6047;
         case 7:
            return 7537;
         case 8:
            return 8467;
         case 9:
            return 9973;
         default:
            throw new IllegalArgumentException("Only support up to 10 hash functions");
      }
   }
}
