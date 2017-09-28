package org.infinispan.counter.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.logging.Log;

/**
 * {@link org.infinispan.counter.api.WeakCounter} configuration.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class WeakCounterConfiguration extends AbstractCounterConfiguration {

   private static final Log log = LogFactory.getLog(WeakCounterConfiguration.class, Log.class);
   static final AttributeDefinition<Integer> CONCURRENCY_LEVEL = AttributeDefinition
         .builder("concurrencyLevel", 16)
         .xmlName("concurrency-level")
         .validator(value -> {
            if (value < 1) {
               throw log.invalidConcurrencyLevel(value);
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
