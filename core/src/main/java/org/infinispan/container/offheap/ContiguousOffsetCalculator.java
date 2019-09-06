package org.infinispan.container.offheap;

/**
 * OffsetCalculator that provides an offset where it is calculated by dividing the positive int values into contiguous
 * blocks. As an example if the int hash spectrum was 8 and the numBlocks provided was 2, then the blocks would contain
 * 0-3 and 4-7 which map to offset 0 and 1 respectively.
 * @author wburns
 * @since 9.4
 */
class ContiguousOffsetCalculator implements OffsetCalculator {
   // usable bits of normal node hash (only allow positive numbers)
   private static final int HASH_BITS = 0x7fffffff;

   private final int offsetShift;

   /**
    * Creates a new contiguous offset calculator using the provided offset segment the entire positive int spectrum
    * up to {@link Integer#MAX_VALUE}.
    * @param numBlocks how many blocks there will be - note this <b>MUST</b> be a power of two
    */
   ContiguousOffsetCalculator(int numBlocks) {
      // We need a positive offset count
      // And it has to be a power of 2
      if (numBlocks <= 0 || (numBlocks & (numBlocks - 1)) != 0) {
         throw new IllegalArgumentException("numBlocks " + numBlocks + " must be greater than 0 and a power of 2");
      }
      // Max capacity is 2^31 (thus find the bit position that would be like dividing evenly into that)
      offsetShift = 31 - Integer.numberOfTrailingZeros(numBlocks);
   }

   @Override
   public int calculateOffsetUsingHashCode(int hashCode) {
      // Since divisor is power of 2, we can just right shift
      return spread(hashCode) >>> offsetShift;
   }

   private static int spread(int h) {
      // Spread using fibonacci hash (using golden ratio)
      // This number is ((2^31 -1) / 1.61803398875) - then rounded to nearest odd number
      // We want something that will prevent hashCodes that are near each other being in the same bucket but still fast
      // We then force the number to be positive by throwing out the first bit
      return (h * 1327217885) & HASH_BITS;
   }
}
