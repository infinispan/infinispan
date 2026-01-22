package org.infinispan.commons.spi;

/**
 * Scalar implementation of {@link Vectors}.
 */
public class ScalarVectors implements Vectors {
   public static final ScalarVectors INSTANCE = new ScalarVectors();
   private static final String HEX_VALUES = "0123456789ABCDEF";

   private ScalarVectors() {
   }

   @Override
   public boolean arraysEqual(byte[] a, int aFromIndex, int aToIndex, byte[] b, int bFromIndex, int bToIndex) {
      int totalAmount = aToIndex - aFromIndex;
      if (totalAmount != bToIndex - bFromIndex) {
         return false;
      }
      for (int i = 0; i < totalAmount; ++i) {
         if (a[aFromIndex + i] != b[bFromIndex + i]) {
            return false;
         }
      }
      return true;
   }

   @Override
   public long minDistance(long segmentHash, long[] nodeHashes) {
      int numNodeHashes = nodeHashes.length;
      // Binary search as in the original code
      int hashIndex = java.util.Arrays.binarySearch(nodeHashes, segmentHash);
      if (hashIndex > 0) {
         return 0L;
      } else {
         hashIndex = -(hashIndex + 1);

         long hashBefore = hashIndex > 0 ? nodeHashes[hashIndex - 1] : nodeHashes[numNodeHashes - 1];
         long hashAfter = hashIndex < numNodeHashes ? nodeHashes[hashIndex] : nodeHashes[0];

         return Math.min(distance(hashBefore, segmentHash), distance(hashAfter, segmentHash));
      }
   }

   private long distance(long a, long b) {
      long distance = a < b ? b - a : a - b;
      if ((distance & (1L << 62)) != 0) {
         distance = -distance - Long.MIN_VALUE;
      }
      return distance;
   }
}
