package org.infinispan.commons.jdkspecific;

import org.infinispan.commons.spi.Vectors;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * Vectorized implementation of {@link Vectors}.
 */
public class VectorizedVectors implements Vectors {
   private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;
   private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;

   @Override
   public boolean arraysEqual(byte[] a, int aFromIndex, int aToIndex, byte[] b, int bFromIndex, int bToIndex) {
      int length = aToIndex - aFromIndex;
      if (length != bToIndex - bFromIndex) {
         return false;
      }

      int i = 0;
      int loopBound = BYTE_SPECIES.loopBound(length);
      for (; i < loopBound; i += BYTE_SPECIES.length()) {
         ByteVector va = ByteVector.fromArray(BYTE_SPECIES, a, aFromIndex + i);
         ByteVector vb = ByteVector.fromArray(BYTE_SPECIES, b, bFromIndex + i);
         if (va.compare(VectorOperators.NE, vb).anyTrue()) {
            return false;
         }
      }

      for (; i < length; i++) {
         if (a[aFromIndex + i] != b[bFromIndex + i]) {
            return false;
         }
      }
      return true;
   }

   @Override
   public long minDistance(long segmentHash, long[] nodeHashes) {
      // Linear scan with vectors
      long minDst = Long.MAX_VALUE;
      int i = 0;
      int length = nodeHashes.length;
      int loopBound = LONG_SPECIES.loopBound(length);

      LongVector segmentVector = LongVector.broadcast(LONG_SPECIES, segmentHash);

      // Accumulate min distances in a vector
      LongVector minVector = LongVector.broadcast(LONG_SPECIES, Long.MAX_VALUE);

      for (; i < loopBound; i += LONG_SPECIES.length()) {
         LongVector nodes = LongVector.fromArray(LONG_SPECIES, nodeHashes, i);

         // Calculate distance(nodes, segment)
         // Logic: distance = a < b ? b - a : a - b;
         // Vector API: lanewise min/max or sub

         // nodes < segment ? segment - nodes : nodes - segment
         // = abs(nodes - segment)
         LongVector diff = nodes.sub(segmentVector).abs();

         // Check for ring overflow: if ((distance & (1L << 62)) != 0)
         LongVector overflowMask = diff.and(1L << 62);
         // if overflow, distance = -distance - Long.MIN_VALUE
         // -distance is 0 - distance

         LongVector corrected = diff.neg().sub(Long.MIN_VALUE);

         // Select based on mask
         // mask != 0 ? corrected : diff
         diff = diff.blend(corrected, overflowMask.compare(VectorOperators.NE, 0));

         minVector = minVector.min(diff);
      }

      // Reduce minVector
      minDst = minVector.reduceLanes(VectorOperators.MIN);
      // Scalar tail
      for (; i < length; i++) {
         long dst = distance(nodeHashes[i], segmentHash);
         if (dst < minDst) {
            minDst = dst;
         }
      }

      return minDst;
   }

   private long distance(long a, long b) {
      long distance = a < b ? b - a : a - b;
      if ((distance & (1L << 62)) != 0) {
         distance = -distance - Long.MIN_VALUE;
      }
      return distance;
   }
}
