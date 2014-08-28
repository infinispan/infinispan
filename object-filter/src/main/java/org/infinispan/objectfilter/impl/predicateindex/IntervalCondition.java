package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.util.Interval;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class IntervalCondition<AttributeDomain extends Comparable<AttributeDomain>> extends Condition<AttributeDomain> {

   private final Interval<AttributeDomain> interval;

   public IntervalCondition(Interval<AttributeDomain> interval) {
      this.interval = interval;
   }

   @Override
   public boolean match(AttributeDomain attributeValue) {
      return attributeValue != null && interval.contains(attributeValue);
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;

      IntervalCondition other = (IntervalCondition) obj;
      return interval.equals(other.interval);
   }

   @Override
   public int hashCode() {
      return interval.hashCode();
   }

   @Override
   public String toString() {
      return "IntervalCondition(" + interval + ')';
   }
}
