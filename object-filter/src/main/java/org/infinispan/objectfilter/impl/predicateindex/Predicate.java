package org.infinispan.objectfilter.impl.predicateindex;

/**
 * A predicate attached to an attribute. It comes in two flavors: condition predicate or interval predicate. An interval
 * predicate represents a range of values (possibly infinite at one end but not both). It requires that the attribute
 * domain is Comparable, otherwise the notion of interval is meaningless. A condition predicate on the other hand can
 * have any arbitrary condition and does not require the attribute value to be Comparable.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public class Predicate<AttributeDomain> {

   protected AttributeNode attributeNode;

   /**
    * Indicates if this predicate is attached to a repeated attribute (one of the attribute path components is a
    * collection/array) and thus it will have multiple evaluations.
    */
   private final boolean isRepeated;

   /**
    * The condition, never null. There is always an equivalent condition even for interval predicates.
    */
   private final Condition<AttributeDomain> condition;

   public Predicate(boolean isRepeated, Condition<AttributeDomain> condition) {
      this.isRepeated = isRepeated;
      this.condition = condition;
   }

   public boolean isRepeated() {
      return isRepeated;
   }

   public boolean match(AttributeDomain attributeValue) {
      return condition.match(attributeValue);
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;

      Predicate other = (Predicate) obj;
      return attributeNode == other.attributeNode
            && isRepeated == other.isRepeated
            && condition.equals(other.condition);
   }

   @Override
   public int hashCode() {
      int i = condition.hashCode();
      i = i + 31 * attributeNode.hashCode();
      return 31 * i + (isRepeated ? 1 : 0);
   }

   @Override
   public String toString() {
      return "Predicate(" + attributeNode + ", isRepeated=" + isRepeated + ", " + condition + ")";
   }
}
