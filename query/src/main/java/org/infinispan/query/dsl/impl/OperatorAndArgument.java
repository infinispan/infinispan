package org.infinispan.query.dsl.impl;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
abstract class OperatorAndArgument<ArgumentType> implements Visitable {

   protected final AttributeCondition attributeCondition;

   protected final ArgumentType argument;

   protected OperatorAndArgument(AttributeCondition attributeCondition, ArgumentType argument) {
      this.attributeCondition = attributeCondition;
      this.argument = argument;
   }

   AttributeCondition getAttributeCondition() {
      return attributeCondition;
   }

   ArgumentType getArgument() {
      return argument;
   }

   //todo [anistor] must also validate that the argument type is compatible with the operator
   void validate() {
      if (argument == null) {
         throw new IllegalArgumentException("argument cannot be null");
      }
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{argument=" + argument + '}';
   }
}
