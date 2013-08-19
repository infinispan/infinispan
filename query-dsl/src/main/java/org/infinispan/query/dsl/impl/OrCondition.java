package org.infinispan.query.dsl.impl;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class OrCondition extends BooleanCondition {

   public OrCondition(BaseCondition leftCondition, BaseCondition rightCondition) {
      super(leftCondition, rightCondition);
   }

   @Override
   public <ReturnType> ReturnType accept(Visitor<ReturnType> visitor) {
      return visitor.visit(this);
   }

   @Override
   public String toString() {
      return "(" + getFirstCondition() + ") OR (" + getSecondCondition() + ")";
   }
}
