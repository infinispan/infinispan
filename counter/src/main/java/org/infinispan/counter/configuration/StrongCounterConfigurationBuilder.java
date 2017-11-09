package org.infinispan.counter.configuration;

import static org.infinispan.counter.util.Utils.isValid;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.logging.Log;

/**
 * {@link org.infinispan.counter.api.StrongCounter} configuration builder.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class StrongCounterConfigurationBuilder extends
      AbstractCounterConfigurationBuilder<StrongCounterConfiguration, StrongCounterConfigurationBuilder> {

   private static final Log log = LogFactory.getLog(StrongCounterConfigurationBuilder.class, Log.class);

   StrongCounterConfigurationBuilder(CounterManagerConfigurationBuilder builder) {
      super(builder, StrongCounterConfiguration.attributeDefinitionSet());
   }

   /**
    * Sets the upper bound (inclusive) of the counter if a bounded counter is desired.
    * <p>
    * Default value is {@link Long#MAX_VALUE}.
    *
    * @param value the new counter's upper bound.
    */
   public StrongCounterConfigurationBuilder upperBound(long value) {
      attributes.attribute(StrongCounterConfiguration.UPPER_BOUND).set(value);
      return self();
   }

   /**
    * Sets the lower bound (inclusive) of the counter if a bounded counter is desired.
    * <p>
    * Default value is {@link Long#MIN_VALUE}.
    *
    * @param value the new counter's lower bound.
    */
   public StrongCounterConfigurationBuilder lowerBound(long value) {
      attributes.attribute(StrongCounterConfiguration.LOWER_BOUND).set(value);
      return self();
   }

   @Override
   public StrongCounterConfiguration create() {
      return new StrongCounterConfiguration(attributes);
   }

   @Override
   public Builder<?> read(StrongCounterConfiguration template) {
      this.attributes.read(template.attributes());
      return self();
   }

   @Override
   public StrongCounterConfigurationBuilder self() {
      return this;
   }

   @Override
   public void validate() {
      super.validate();
      if (attributes.attribute(StrongCounterConfiguration.LOWER_BOUND).isModified() || attributes.attribute(
            StrongCounterConfiguration.UPPER_BOUND).isModified()) {
         long min = attributes.attribute(StrongCounterConfiguration.LOWER_BOUND).get();
         long max = attributes.attribute(StrongCounterConfiguration.UPPER_BOUND).get();
         long init = attributes.attribute(StrongCounterConfiguration.INITIAL_VALUE).get();
         if (isValid(min, init, max)) {
            throw log.invalidInitialValueForBoundedCounter(min, max, init);
         }
      }
   }
}
