package org.infinispan.counter.configuration;

import static org.infinispan.counter.configuration.Element.STRONG_COUNTER;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * {@link org.infinispan.counter.api.StrongCounter} configuration.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class StrongCounterConfiguration extends AbstractCounterConfiguration {

   static final AttributeDefinition<Long> LOWER_BOUND = AttributeDefinition.builder("lowerBound", Long.MIN_VALUE)
         .immutable()
         .build();

   static final AttributeDefinition<Long> UPPER_BOUND = AttributeDefinition.builder("upperBound", Long.MAX_VALUE)
         .immutable()
         .build();

   StrongCounterConfiguration(AttributeSet attributes) {
      super(attributes);
   }

   static ElementDefinition<StrongCounterConfiguration> ELEMENT_DEFINITION = new DefaultElementDefinition<>(STRONG_COUNTER.toString());

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(StrongCounterConfiguration.class, AbstractCounterConfiguration.attributeDefinitionSet(),
            LOWER_BOUND, UPPER_BOUND);
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
}
