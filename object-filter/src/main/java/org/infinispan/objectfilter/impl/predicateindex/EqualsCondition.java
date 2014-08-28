package org.infinispan.objectfilter.impl.predicateindex;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class EqualsCondition<AttributeDomain> extends Condition<AttributeDomain> {

   private final AttributeDomain value;

   public EqualsCondition(AttributeDomain value) {
      if (value == null) {
         throw new IllegalArgumentException("value cannot be null");
      }
      this.value = value;
   }

   @Override
   public boolean match(Object attributeValue) {
      return value.equals(attributeValue);
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;

      EqualsCondition other = (EqualsCondition) obj;
      return value.equals(other.value);
   }

   @Override
   public int hashCode() {
      return value.hashCode();
   }

   @Override
   public String toString() {
      return "EqualsCondition(" + value + ')';
   }
}
