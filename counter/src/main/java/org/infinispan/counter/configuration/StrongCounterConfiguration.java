package org.infinispan.counter.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * {@link org.infinispan.counter.api.StrongCounter} configuration.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class StrongCounterConfiguration extends AbstractCounterConfiguration {

   static final AttributeDefinition<Long> LOWER_BOUND = AttributeDefinition.builder(Attribute.LOWER_BOUND, Long.MIN_VALUE)
         .immutable()
         .build();

   static final AttributeDefinition<Long> UPPER_BOUND = AttributeDefinition.builder(Attribute.UPPER_BOUND, Long.MAX_VALUE)
         .immutable()
         .build();

   static final AttributeDefinition<Long> LIFESPAN = AttributeDefinition.builder(Attribute.LIFESPAN, -1L)
         .immutable()
         .build();

   StrongCounterConfiguration(AttributeSet attributes) {
      super(attributes);
   }

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(StrongCounterConfiguration.class, AbstractCounterConfiguration.attributeDefinitionSet(),
            LOWER_BOUND, UPPER_BOUND, LIFESPAN);
   }

   /**
    * @return {@code true} if the counter is bounded (lower and/or upper bound has been set), {@code false} otherwise.
    */
   public boolean isBound() {
      return attributes.attribute(LOWER_BOUND).isModified() || attributes.attribute(UPPER_BOUND).isModified();
   }

   public long lowerBound() {
      return attributes.attribute(LOWER_BOUND).get();
   }

   public long upperBound() {
      return attributes.attribute(UPPER_BOUND).get();
   }

   public long lifespan() {
      return attributes.attribute(LIFESPAN).get();
   }
}
