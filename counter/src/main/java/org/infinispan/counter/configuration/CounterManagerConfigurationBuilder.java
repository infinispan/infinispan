package org.infinispan.counter.configuration;

import static org.infinispan.counter.logging.Log.CONTAINER;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

/**
 * The {@link org.infinispan.counter.api.CounterManager} configuration builder.
 * <p>
 * It configures the number of owner and the {@link Reliability} mode. It allow to configure the default counter
 * available on startup.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class CounterManagerConfigurationBuilder implements Builder<CounterManagerConfiguration> {

   private static final CounterManagerConfiguration DEFAULT = new CounterManagerConfigurationBuilder(null).create();

   private final AttributeSet attributes = CounterManagerConfiguration.attributeDefinitionSet();
   private final List<CounterConfigurationBuilder<?, ?>> defaultCounters;
   private final GlobalConfigurationBuilder builder;

   @SuppressWarnings("WeakerAccess")
   public CounterManagerConfigurationBuilder(GlobalConfigurationBuilder builder) {
      defaultCounters = new ArrayList<>(2);
      this.builder = builder;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * @return the default {@link CounterManagerConfiguration}.
    */
   public static CounterManagerConfiguration defaultConfiguration() {
      return DEFAULT;
   }

   /**
    * Sets the number of copies of the counter's value available in the cluster.
    * <p>
    * A higher value will provide better availability at the cost of more expensive updates.
    * <p>
    * Default value is 2.
    *
    * @param numOwners the number of copies.
    */
   public CounterManagerConfigurationBuilder numOwner(int numOwners) {
      attributes.attribute(CounterManagerConfiguration.NUM_OWNERS).set(numOwners);
      return this;
   }

   /**
    * Sets the {@link Reliability} mode.
    * <p>
    * Default value is {@link Reliability#AVAILABLE}.
    *
    * @param reliability the {@link Reliability} mode.
    * @see Reliability
    */
   public CounterManagerConfigurationBuilder reliability(Reliability reliability) {
      attributes.attribute(CounterManagerConfiguration.RELIABILITY).set(reliability);
      return this;
   }

   /**
    * @return a new {@link StrongCounterConfigurationBuilder} to configure a strong consistent counters.
    */
   public StrongCounterConfigurationBuilder addStrongCounter() {
      StrongCounterConfigurationBuilder builder = new StrongCounterConfigurationBuilder(this);
      defaultCounters.add(builder);
      return builder;
   }

   /**
    * @return a new {@link WeakCounterConfigurationBuilder} to configure weak consistent counters.
    */
   public WeakCounterConfigurationBuilder addWeakCounter() {
      WeakCounterConfigurationBuilder builder = new WeakCounterConfigurationBuilder(this);
      defaultCounters.add(builder);
      return builder;
   }

   @Override
   public void validate() {
      attributes.attributes().forEach(Attribute::validate);
      defaultCounters.forEach(CounterConfigurationBuilder::validate);
      Set<String> counterNames = new HashSet<>();
      for (CounterConfigurationBuilder builder : defaultCounters) {
         if (!counterNames.add(builder.name())) {
            throw CONTAINER.duplicatedCounterName(builder.name());
         }
      }
   }

   @Override
   public CounterManagerConfiguration create() {
      Map<String, AbstractCounterConfiguration> counters = new HashMap<>(defaultCounters.size());
      for (CounterConfigurationBuilder<?, ?> builder : defaultCounters) {
         counters.put(builder.name(), builder.create());
      }
      return new CounterManagerConfiguration(attributes.protect(), counters);
   }

   @Override
   public Builder<?> read(CounterManagerConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      return this;
   }

   /**
    * Clears all the configured counters.
    */
   public void clearCounters() {
      defaultCounters.clear();
   }

   /**
    * @return {@code true} if global state is enabled, {@link false} otherwise.
    */
   boolean isGlobalStateEnabled() {
      return builder.globalState().enabled();
   }

   public List<CounterConfigurationBuilder<?,?>> counters() {
      return defaultCounters;
   }
}
