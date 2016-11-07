package org.infinispan.util;

import org.infinispan.commons.equivalence.Equivalence;

/**
 * An hash function for stripping.
 * <p>
 * It calculates the number of segments based on the concurrency level desired and hashes the object to the
 * corresponding segments.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class StripedHashFunction<T> {

   private final int lockSegmentMask;
   private final int lockSegmentShift;
   private final int numSegments;

   public StripedHashFunction(int concurrencyLevel) {
      int tempLockSegShift = 0;
      int tmpNumSegments = 1;
      while (tmpNumSegments < concurrencyLevel) {
         ++tempLockSegShift;
         tmpNumSegments <<= 1;
      }
      lockSegmentShift = 32 - tempLockSegShift;
      lockSegmentMask = tmpNumSegments - 1;
      numSegments = tmpNumSegments;
   }

   /**
    * @param hashCode the object's hash code serving as a key.
    * @return the hash code of the key
    */
   private static int hash(int hashCode) {
      int h = hashCode;
      h += ~(h << 9);
      h ^= (h >>> 14);
      h += (h << 4);
      h ^= (h >>> 10);
      return h;
   }

   /**
    * @return the number of segments.
    */
   public final int getNumSegments() {
      return numSegments;
   }

   /**
    * It calculates the segment in which the object belongs.
    *
    * @param object the object to hash.
    * @return the segment index, between 0 and {@link #getNumSegments()}-1.
    */
   public final int hashToSegment(T object) {
      return (hash(object.hashCode()) >>> lockSegmentShift) & lockSegmentMask;
   }
}
