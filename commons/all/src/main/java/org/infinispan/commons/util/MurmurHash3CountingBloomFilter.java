package org.infinispan.commons.util;

/**
 * {@link CountingBloomFilter} implementation that uses the very same MurmurHash3 based hash functions as
 * {@link MurmurHash3BloomFilter}. A filter created with a given number of counters and hash functions therefore
 * hashes values to the exact same positions as the plain bloom filter with the same arguments, which allows the two
 * to be used interchangeably.
 * <p>
 * The default number of hash functions is 3.
 *
 * @since 16.3
 */
public class MurmurHash3CountingBloomFilter extends CountingBloomFilter<byte[]> {
   MurmurHash3CountingBloomFilter(int counterCount, int hashFunctions) {
      super(counterCount, (Iterable) MurmurHash3BloomFilter.functions(hashFunctions));
   }

   public static CountingBloomFilter<byte[]> createFilter(int counterCount) {
      return createFilter(counterCount, MurmurHash3BloomFilter.defaultHashFunctionCount());
   }

   public static CountingBloomFilter<byte[]> createFilter(int counterCount, int hashFunctions) {
      return new MurmurHash3CountingBloomFilter(counterCount, hashFunctions);
   }
}
