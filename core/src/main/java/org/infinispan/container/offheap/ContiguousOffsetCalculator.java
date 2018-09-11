package org.infinispan.container.offheap;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;

/**
 * OffsetCalculator that provides an offset where it is calculated by dividing the positive int values into contiguous
 * blocks. As an example if the int hash spectrum was 8 and the numBlocks provided was 2, then the blocks would contain
 * 0-3 and 4-7 which map to offset 0 and 1 respectively.
 * @author wburns
 * @since 9.4
 */
class ContiguousOffsetCalculator implements OffsetCalculator {
   // This is the max capacity when unsigned (thus any number must be this less 1)
   private static final int MAXIMUM_CAPACITY = 1 << 31;
   // usable bits of normal node hash (only allow positive numbers)
   private static final int HASH_BITS = 0x7fffffff;

   private final int offsetDivisor;

   // We use murmur hash as simple spread doesn't work well with contiguous blocks (similar hash codes end up in the
   // same block otherwise)
   private final static Hash hash = MurmurHash3.getInstance();

   /**
    * Creates a new contiguous offset calculator using the provided offset segment the entire positive int spectrum
    * up to {@link Integer#MAX_VALUE}.
    * @param numBlocks how many blocks there will be - note this <b>MUST</b> be a power of two
    */
   ContiguousOffsetCalculator(int numBlocks) {
      // We need a positive offset count
      // And it has to be a power of 2
      if (numBlocks <= 0 && (numBlocks & (numBlocks - 1)) == 0) {
         throw new IllegalArgumentException("maxOffset " + numBlocks + " must be greater than 0 and a power of 2");
      }
      // Max capacity is negative (unsigned 2^32)
      offsetDivisor = MAXIMUM_CAPACITY >>> Integer.numberOfTrailingZeros(numBlocks);
   }

   @Override
   public int calculateOffsetUsingHashCode(int hashCode) {
      return spread(hashCode) / offsetDivisor;
   }

   private static int spread(int h) {
      // Spread using murmur hash then ensure it is positive before finding block to use
      return hash.hash(h) & HASH_BITS;
   }
}
