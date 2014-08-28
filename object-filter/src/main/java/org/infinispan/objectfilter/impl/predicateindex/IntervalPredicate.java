package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.util.Interval;

/**
 * A predicate attached to an attribute. It comes in two flavors: condition predicate or interval predicate. An interval
 * predicate represents a range of values (possibly infinite at one end but not both). It requires that the attribute
 * domain is Comparable, otherwise the notion of interval is meaningless. A condition predicate on the other hand can
 * have any arbitrary condition and does not require the attribute value to be Comparable.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class IntervalPredicate<AttributeDomain extends Comparable<AttributeDomain>> extends Predicate<AttributeDomain> {

   /**
    * The interval.
    */
   private final Interval<AttributeDomain> interval;

   public IntervalPredicate(boolean isRepeated, Interval<AttributeDomain> interval) {
      super(isRepeated, new IntervalCondition<AttributeDomain>(interval));
      this.interval = interval;
   }

   public Interval<AttributeDomain> getInterval() {
      return interval;
   }
}
