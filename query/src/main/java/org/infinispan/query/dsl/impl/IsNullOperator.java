package org.infinispan.query.dsl.impl;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class IsNullOperator extends OperatorAndArgument<Void> {

   protected IsNullOperator(AttributeCondition parentCondition) {
      super(parentCondition, null);
   }

   @Override
   public <ReturnType> ReturnType accept(Visitor<ReturnType> visitor) {
      return visitor.visit(this);
   }

   @Override
   void validate() {
      // no validation rules for 'isNull'
   }
}
