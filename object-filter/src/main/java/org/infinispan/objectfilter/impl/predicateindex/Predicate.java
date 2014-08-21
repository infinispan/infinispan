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
public final class Predicate<AttributeDomain> {

   public interface Callback {

      void handleValue(MatcherEvalContext<?> ctx, boolean isMatching);
   }

   protected AttributeNode attributeNode;

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
      return attributeNode == other.attributeNode && (interval != null ? interval.equals(other.interval) : condition.equals(other.condition));
   }

   @Override
   public int hashCode() {
      int i = interval != null ? interval.hashCode() : condition.hashCode();
      return i + 31 * attributeNode.hashCode();
   }

   @Override
   public String toString() {
      return "Predicate(" + attributeNode + ", " + (interval != null ? interval.toString() : condition.toString()) + ")";
   }
}
