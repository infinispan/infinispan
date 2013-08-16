package org.infinispan.query.dsl.impl;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class BetweenOperator extends OperatorAndArgument<ValueRange> {

   protected BetweenOperator(AttributeCondition parentCondition, ValueRange argument) {
      super(parentCondition, argument);
   }

   @Override
   public <ReturnType> ReturnType accept(Visitor<ReturnType> visitor) {
      return visitor.visit(this);
   }
}
