package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.impl.logging.Log;
import org.jboss.logging.Logger;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
abstract class OperatorAndArgument<ArgumentType> implements Visitable {

   private static final Log log = Logger.getMessageLogger(Log.class, OperatorAndArgument.class.getName());

   protected final AttributeCondition attributeCondition;

   protected final ArgumentType argument;

   protected OperatorAndArgument(AttributeCondition attributeCondition, ArgumentType argument) {
      if (attributeCondition == null) {
         throw log.argumentCannotBeNull("attributeCondition");
      }
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
         throw log.argumentCannotBeNull();
      }
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{argument=" + argument + '}';
   }
}
