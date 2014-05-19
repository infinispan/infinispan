package org.infinispan.objectfilter.impl.predicateindex;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class IsNullCondition extends Condition<Object> {

   public static final IsNullCondition INSTANCE = new IsNullCondition();

   private IsNullCondition() {
   }

   @Override
   public boolean match(Object attributeValue) {
      return attributeValue == null;
   }

   @Override
   public String toString() {
      return "IsNullCondition";
   }
}