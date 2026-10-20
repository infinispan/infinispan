package org.infinispan.commons.util;

/**
 * {@link CountingBloomFilter} implementation using the very same hash functions as {@link MurmurHash3BloomFilter},
 * which means a filter of either kind can be built on one side of a connection and the resulting bits exchanged with
 * the other side.
 * <p>
 * The default number of hash functions is 3.
 *
 * @since 16.3
 */
public class MurmurHash3CountingBloomFilter extends CountingBloomFilter<byte[]> {
   MurmurHash3CountingBloomFilter(int bitsToUse, int hashFunctions) {
      super(bitsToUse, MurmurHash3BloomFilter.functions(hashFunctions));
   }

   public static CountingBloomFilter<byte[]> createFilter(int bitsToUse) {
      return createFilter(bitsToUse, MurmurHash3BloomFilter.defaultHashFunctionCount());
   }

   public static CountingBloomFilter<byte[]> createFilter(int bitsToUse, int hashFunctions) {
      return new MurmurHash3CountingBloomFilter(bitsToUse, hashFunctions);
   }
}
