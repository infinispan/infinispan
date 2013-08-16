package org.infinispan.query.dsl.impl;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class NotCondition extends BooleanCondition {

   public NotCondition(BaseCondition condition) {
      super(condition, null);
   }

   @Override
   public <ReturnType> ReturnType accept(Visitor<ReturnType> visitor) {
      return visitor.visit(this);
   }

   @Override
   public String toString() {
      return "NOT (" + getFirstCondition();
   }
}
