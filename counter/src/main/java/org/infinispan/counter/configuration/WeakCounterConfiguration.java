package org.infinispan.counter.configuration;

import static org.infinispan.counter.logging.Log.CONTAINER;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * {@link org.infinispan.counter.api.WeakCounter} configuration.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class WeakCounterConfiguration extends AbstractCounterConfiguration {

   static final AttributeDefinition<Integer> CONCURRENCY_LEVEL = AttributeDefinition
         .builder(Attribute.CONCURRENCY_LEVEL, 16)
         .validator(value -> {
            if (value < 1) {
               throw CONTAINER.invalidConcurrencyLevel(value);
            }
         })
         .immutable()
         .build();

   WeakCounterConfiguration(AttributeSet attributes) {
      super(attributes);
   }

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(WeakCounterConfiguration.class, AbstractCounterConfiguration.attributeDefinitionSet(),
            CONCURRENCY_LEVEL);
   }

   public int concurrencyLevel() {
      return attributes.attribute(CONCURRENCY_LEVEL).get();
   }
}
