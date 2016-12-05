package org.infinispan.counter.configuration;

import java.util.Collections;
import java.util.List;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.serializing.SerializedWith;
import org.infinispan.counter.logging.Log;

/**
 * The {@link org.infinispan.counter.api.CounterManager} configuration.
 * <p>
 * It configures the number of owners (number of copies in the cluster) of a counter and the {@link Reliability} mode.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@SerializedWith(CounterConfigurationSerializer.class)
public class CounterManagerConfiguration {

   private static final Log log = LogFactory.getLog(CounterManagerConfiguration.class, Log.class);
   static final AttributeDefinition<Reliability> RELIABILITY = AttributeDefinition
         .builder("reliability", Reliability.AVAILABLE)
         .validator(value -> {
            if (value == null) {
               throw log.invalidReliabilityMode();
            }
         })
         .immutable().build();
   static final AttributeDefinition<Integer> NUM_OWNERS = AttributeDefinition.builder("numOwners", 2)
         .validator(value -> {
            if (value < 1) {
               throw log.invalidNumOwners(value);
            }
         })
         .immutable().build();
   private final AttributeSet attributes;
   private final List<? extends AbstractCounterConfiguration> counters;

   CounterManagerConfiguration(AttributeSet attributes, List<? extends AbstractCounterConfiguration> counters) {
      this.attributes = attributes;
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

   AttributeSet attributes() {
      return attributes;
   }

   public List<AbstractCounterConfiguration> counters() {
      return Collections.unmodifiableList(counters);
   }
}
