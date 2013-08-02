package org.infinispan.query.dsl.impl;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class LikeOperator extends OperatorAndArgument<String> {

   protected LikeOperator(AttributeCondition parentCondition, String argument) {
      super(parentCondition, argument);
   }

   @Override
   public <ReturnType> ReturnType accept(Visitor<ReturnType> visitor) {
      return visitor.visit(this);
   }
}
