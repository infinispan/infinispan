package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.interceptors.base.CommandInterceptor;

/**
 * Describes a custom interceptor
 */
public class InterceptorConfiguration extends AbstractTypedPropertiesConfiguration {
   /**
    * Positional placing of a new custom interceptor
    */
   public static enum Position {
      /**
       * Specifies that the new interceptor is placed first in the chain.
       */
      FIRST,
      /**
       * Specifies that the new interceptor is placed last in the chain. The new interceptor is added right before the
       * last interceptor in the chain. The very last interceptor is owned by Infinispan and cannot be replaced.
       */
      LAST,
      /**
       * Specifies that the new interceptor can be placed anywhere, except first or last.
       */
      OTHER_THAN_FIRST_OR_LAST
   }

   static final AttributeDefinition<Position> POSITION = AttributeDefinition.builder("position", Position.OTHER_THAN_FIRST_OR_LAST).immutable().build();
   static final AttributeDefinition<Class> AFTER = AttributeDefinition.builder("after", null, Class.class).immutable().build();
   static final AttributeDefinition<Class> BEFORE = AttributeDefinition.builder("before", null, Class.class).immutable().build();
   static final AttributeDefinition<CommandInterceptor> INTERCEPTOR = AttributeDefinition.builder("interceptor", null, CommandInterceptor.class).immutable().build();
   static final AttributeDefinition<Integer> INDEX = AttributeDefinition.builder("index", -1).immutable().build();

   public static AttributeSet attributeSet() {
      return new AttributeSet(InterceptorConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(), POSITION, AFTER, BEFORE, INTERCEPTOR, INDEX);
   }

   InterceptorConfiguration(AttributeSet attributes) {
      super(attributes);
   }

   @SuppressWarnings("unchecked")
   public Class<? extends CommandInterceptor> after() {
      return attributes.attribute(AFTER).asObject(Class.class);
   }

   @SuppressWarnings("unchecked")
   public Class<? extends CommandInterceptor> before() {
      return attributes.attribute(BEFORE).asObject(Class.class);
   }

   public CommandInterceptor interceptor() {
      return attributes.attribute(INTERCEPTOR).asObject(CommandInterceptor.class);
   }

   public int index() {
      return attributes.attribute(INDEX).asInteger();
   }

   public Position position() {
      return attributes.attribute(POSITION).asObject(Position.class);
   }

   public boolean first() {
      return position() == Position.FIRST;
   }

   public boolean last() {
      return position() == Position.LAST;
   }

   AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "InterceptorConfiguration [attributes=" + attributes + "]";
   }
}
