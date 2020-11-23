package org.infinispan.commons.util;

import java.util.Objects;

/**
 * Encapsulates the key and its segment.
 *
 * @author Pedro Ruivo
 * @since 12
 */
public class SegmentAwareKey<K> {

   private final K key;
   private final int segment;

   public SegmentAwareKey(K key, int segment) {
      this.key = Objects.requireNonNull(key);
      this.segment = segment;
   }

   public K getKey() {
      return key;
   }

   public int getSegment() {
      return segment;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SegmentAwareKey<?> that = (SegmentAwareKey<?>) o;
      return segment == that.segment && key.equals(that.key);
   }

   @Override
   public int hashCode() {
      return Objects.hash(key, segment);
   }

   @Override
   public String toString() {
      return "SegmentAwareKey{" +
            "key=" + Util.toStr(key) +
            ", segment=" + segment +
            '}';
   }
}
