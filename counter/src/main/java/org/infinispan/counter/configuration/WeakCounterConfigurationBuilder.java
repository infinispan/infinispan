package org.infinispan.counter.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * {@link org.infinispan.counter.api.WeakCounter} configuration builder.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class WeakCounterConfigurationBuilder extends
      AbstractCounterConfigurationBuilder<WeakCounterConfiguration, WeakCounterConfigurationBuilder> {

   public WeakCounterConfigurationBuilder(CounterManagerConfigurationBuilder builder) {
      super(builder, WeakCounterConfiguration.attributeDefinitionSet());
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public WeakCounterConfiguration create() {
      return new WeakCounterConfiguration(attributes);
   }

   @Override
   public Builder<?> read(WeakCounterConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public WeakCounterConfigurationBuilder self() {
      return this;
   }

   /**
    * Sets the counter's concurrency level.
    * <p>
    * It sets the number of concurrent updates in the counter. A higher value will support a higher number of updates
    * but it increases the read of the counter's value.
    * <p>
    * Default value is 16.
    *
    * @param level the new concurrency level.
    */
   public WeakCounterConfigurationBuilder concurrencyLevel(int level) {
      attributes.attribute(WeakCounterConfiguration.CONCURRENCY_LEVEL).set(level);
      return self();
   }
}
