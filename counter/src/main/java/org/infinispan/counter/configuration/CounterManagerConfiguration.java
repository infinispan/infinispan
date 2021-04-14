package org.infinispan.counter.configuration;

import static org.infinispan.counter.logging.Log.CONTAINER;

import java.util.Collections;
import java.util.Map;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.serializing.SerializedWith;

/**
 * The {@link org.infinispan.counter.api.CounterManager} configuration.
 * <p>
 * It configures the number of owners (number of copies in the cluster) of a counter and the {@link Reliability} mode.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@SerializedWith(CounterConfigurationSerializer.class)
public class CounterManagerConfiguration extends ConfigurationElement<CounterManagerConfiguration> {
   static final AttributeDefinition<Reliability> RELIABILITY = AttributeDefinition
         .builder("reliability", Reliability.AVAILABLE)
         .validator(value -> {
            if (value == null) {
               throw CONTAINER.invalidReliabilityMode();
            }
         })
         .immutable().build();
   static final AttributeDefinition<Integer> NUM_OWNERS = AttributeDefinition.builder(Attribute.NUM_OWNERS, 2)
         .validator(value -> {
            if (value < 1) {
               throw CONTAINER.invalidNumOwners(value);
            }
         })
         .immutable().build();
   private final Map<String, AbstractCounterConfiguration> counters;

   CounterManagerConfiguration(AttributeSet attributes, Map<String, AbstractCounterConfiguration> counters) {
      super(Element.COUNTERS, attributes);
      this.counters = counters;
   }

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(CounterManagerConfiguration.class, NUM_OWNERS, RELIABILITY);
   }

   public int numOwners() {
      return attributes.attribute(NUM_OWNERS).get();
   }

   public Reliability reliability() {
      return attributes.attribute(RELIABILITY).get();
   }

   public Map<String, AbstractCounterConfiguration> counters() {
      return Collections.unmodifiableMap(counters);
   }
}
