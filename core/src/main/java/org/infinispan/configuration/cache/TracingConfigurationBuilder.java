package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.TracingConfiguration.ENABLED;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class TracingConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<TracingConfiguration> {

   private final AttributeSet attributes;

   protected TracingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = TracingConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Enable tracing on the given cache.
    * This property can be used to enable tracing at runtime.
    *
    * @return <code>this</code>, for method chaining
    */
   public TracingConfigurationBuilder enable() {
      attributes.attribute(ENABLED).set(true);
      return this;
   }

   /**
    * Disable tracing on the given cache.
    * This property can be used to disable tracing at runtime.
    *
    * @return <code>this</code>, for method chaining
    */
   public TracingConfigurationBuilder disable() {
      attributes.attribute(ENABLED).set(false);
      return this;
   }

   public TracingConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   @Override
   public TracingConfiguration create() {
      return new TracingConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(TracingConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "TracingConfigurationBuilder{" +
            "attributes=" + attributes +
            '}';
   }
}
