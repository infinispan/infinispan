package org.infinispan.counter.configuration;

import static org.infinispan.counter.impl.Utils.validateStrongCounterBounds;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * {@link org.infinispan.counter.api.StrongCounter} configuration builder.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class StrongCounterConfigurationBuilder extends
      AbstractCounterConfigurationBuilder<StrongCounterConfiguration, StrongCounterConfigurationBuilder> {

   public StrongCounterConfigurationBuilder(CounterManagerConfigurationBuilder builder) {
      super(builder, StrongCounterConfiguration.attributeDefinitionSet());
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return StrongCounterConfiguration.ELEMENT_DEFINITION;
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
         validateStrongCounterBounds(min, init, max);
      }
   }
}
