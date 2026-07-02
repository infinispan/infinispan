package org.infinispan.stats.impl;

/**
 * Segment validation shared by the {@link org.infinispan.stats.HotKeyTracker} implementations, so every tracker
 * accepts and rejects the same segments whether hot key tracking is enabled or not.
 *
 * @since 16.4
 * @author José Bolina
 */
final class TrackerSegments {

   private TrackerSegments() { }

   /**
    * Checks whether a segment exists in a cache with the given number of segments.
    *
    * @param segment     the segment to check
    * @param numSegments the number of segments of the cache
    * @return {@code true} if {@code segment} is between zero (inclusive) and {@code numSegments} (exclusive)
    */
   static boolean isValid(int segment, int numSegments) {
      return segment >= 0 && segment < numSegments;
   }

   /**
    * Validates a segment passed by a caller querying the per-segment counters.
    *
    * @param segment     the segment to validate
    * @param numSegments the number of segments of the cache
    * @return the same {@code segment}, so the call can be used as an array index
    * @throws IllegalArgumentException if {@code segment} is negative or not lower than {@code numSegments}
    */
   static int requireValid(int segment, int numSegments) {
      if (!isValid(segment, numSegments))
         throw new IllegalArgumentException(String.format("Segment %d is out of range [0, %d)", segment, numSegments));
      return segment;
   }
}
