package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.util.Interval;

/**
 * A predicate comes in two flavors: condition predicate or interval predicate.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class Predicate<AttributeDomain> {

   public interface Callback {

      void handleValue(MatcherEvalContext<?> ctx, boolean isMatching);
   }

   // only one of these fields is non-null
   private final Interval<AttributeDomain> interval;    // just for interval predicates

   private final Condition<AttributeDomain> condition;  // just for condition predicates

   public Predicate(Interval<AttributeDomain> interval) {
      this.interval = interval;
      this.condition = null;
   }

   public Predicate(Condition<AttributeDomain> condition) {
      this.interval = null;
      this.condition = condition;
   }

   public Interval<AttributeDomain> getInterval() {
      return interval;
   }

   public Condition<AttributeDomain> getCondition() {
      return condition;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;

      Predicate other = (Predicate) obj;
      return interval != null ? interval.equals(other.interval) : condition.equals(other.condition);
   }

   @Override
   public int hashCode() {
      return interval != null ? interval.hashCode() : condition.hashCode();
   }

   @Override
   public String toString() {
      return "Predicate(" + (interval != null ? interval.toString() : condition.toString()) + ")";
   }
}
