package org.infinispan.query.dsl.impl;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class ContainsAllOperator extends OperatorAndArgument<Object> {

   protected ContainsAllOperator(AttributeCondition parentCondition, Object argument) {
      super(parentCondition, argument);
   }

   @Override
   public <ReturnType> ReturnType accept(Visitor<ReturnType> visitor) {
      return visitor.visit(this);
   }
}
