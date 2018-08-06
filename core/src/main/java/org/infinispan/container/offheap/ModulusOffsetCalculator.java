package org.infinispan.container.offheap;

/**
 * OffsetCalculator that provides an offset where it is calculated by dividing the positive int spectrum into blocks
 * where each subsequent number belongs in the next block rolling over when reaching the max block size. As an example
 * if the int hash spectrum was 8 and the numBlocks provided was 2, then the blocks would contain
 * 0,2,4,6 and 1,3,5,7 which map to offset 0 and 1 respectively.
 * @since 9.4
 */
class ModulusOffsetCalculator implements OffsetCalculator {
   // usable bits of normal node hash (only allow positive numbers)
   private static final int HASH_BITS = 0x7fffffff;

   private final int offsetModulus;

   public ModulusOffsetCalculator(int numBlocks) {
      // We need a positive offset count
      // And it has to be a power of 2
      if (numBlocks <= 0 && (numBlocks & (numBlocks - 1)) == 0) {
         throw new IllegalArgumentException("maxOffset " + numBlocks + " must be greater than 0 and a power of 2");
      }
      // Max capacity is negative (unsigned 2^32)
      this.offsetModulus = numBlocks;
   }

   @Override
   public int calculateOffsetUsingHashCode(int hashCode) {
      return spread(hashCode) % offsetModulus;
   }

   private static int spread(int h) {
      return (h ^ (h >>> 16)) & HASH_BITS;
   }
}
