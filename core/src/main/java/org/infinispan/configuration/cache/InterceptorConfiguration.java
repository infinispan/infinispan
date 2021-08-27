package org.infinispan.configuration.cache;

import static org.infinispan.commons.configuration.attributes.IdentityAttributeCopier.identityCopier;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.interceptors.AsyncInterceptor;

/**
 * Describes a custom interceptor
 */
public class InterceptorConfiguration extends AbstractTypedPropertiesConfiguration {
   /**
    * Positional placing of a new custom interceptor
    */
   public enum Position {
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

   public static final AttributeDefinition<Position> POSITION = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.POSITION, Position.OTHER_THAN_FIRST_OR_LAST).immutable().build();
   public static final AttributeDefinition<Class> AFTER = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.AFTER, null, Class.class)
         .serializer(AttributeSerializer.INSTANCE_CLASS_NAME).immutable().build();
   public static final AttributeDefinition<Class> BEFORE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.BEFORE, null, Class.class)
         .serializer(AttributeSerializer.INSTANCE_CLASS_NAME).immutable().build();
   public static final AttributeDefinition<AsyncInterceptor> INTERCEPTOR = AttributeDefinition.builder(Element.INTERCEPTOR, null, AsyncInterceptor.class)
         .copier(identityCopier()).immutable().build();
   public static final AttributeDefinition<Class> INTERCEPTOR_CLASS = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.CLASS, null, Class.class)
         .serializer(AttributeSerializer.INSTANCE_CLASS_NAME).immutable().build();
   public static final AttributeDefinition<Integer> INDEX = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.INDEX, -1).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(InterceptorConfiguration.class, AbstractTypedPropertiesConfiguration.attributeSet(), POSITION, AFTER, BEFORE, INTERCEPTOR, INTERCEPTOR_CLASS, INDEX);
   }

   private final Attribute<Position> position;
   private final Attribute<Class> after;
   private final Attribute<Class> before;
   private final Attribute<AsyncInterceptor> interceptor;
   private final Attribute<Class> interceptorClass;
   private final Attribute<Integer> index;

   InterceptorConfiguration(AttributeSet attributes) {
      super(attributes);
      position = attributes.attribute(POSITION);
      after = attributes.attribute(AFTER);
      before = attributes.attribute(BEFORE);
      interceptor = attributes.attribute(INTERCEPTOR);
      interceptorClass = attributes.attribute(INTERCEPTOR_CLASS);
      index = attributes.attribute(INDEX);
   }

   @SuppressWarnings("unchecked")
   public Class<? extends AsyncInterceptor> after() {
      return after.get();
   }

   @SuppressWarnings("unchecked")
   public Class<? extends AsyncInterceptor> before() {
      return before.get();
   }

   public AsyncInterceptor asyncInterceptor() {
      if (interceptor.isNull()) {
         return (AsyncInterceptor) Util.getInstance(interceptorClass.get());
      } else {
         return interceptor.get();
      }
   }

   public Class<? extends AsyncInterceptor> sequentialInterceptorClass() {
      return interceptorClass.get();
   }

   public int index() {
      return index.get();
   }

   public Position position() {
      return position.get();
   }

   public boolean first() {
      return position() == Position.FIRST;
   }

   public boolean last() {
      return position() == Position.LAST;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "InterceptorConfiguration [attributes=" + attributes + "]";
   }
}
