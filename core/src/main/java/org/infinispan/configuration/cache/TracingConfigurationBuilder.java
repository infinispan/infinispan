package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.TracingConfiguration.CLUSTER;
import static org.infinispan.configuration.cache.TracingConfiguration.CONTAINER;
import static org.infinispan.configuration.cache.TracingConfiguration.ENABLED;
import static org.infinispan.configuration.cache.TracingConfiguration.X_SITE;

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

   /**
    * Enable or disable tracing for container category on the given cache.
    * This property can be used to enable tracing at runtime.
    *
    * @return <code>this</code>, for method chaining
    */
   public TracingConfigurationBuilder container(boolean enable) {
      attributes.attribute(CONTAINER).set(enable);
      return this;
   }

   /**
    * Enable or disable tracing for cluster category on the given cache.
    * This property can be used to enable tracing at runtime.
    *
    * @return <code>this</code>, for method chaining
    */
   public TracingConfigurationBuilder cluster(boolean enable) {
      attributes.attribute(CLUSTER).set(enable);
      return this;
   }

   /**
    * Enable or disable tracing for x-site category on the given cache.
    * This property can be used to enable tracing at runtime.
    *
    * @return <code>this</code>, for method chaining
    */
   public TracingConfigurationBuilder xSite(boolean enable) {
      attributes.attribute(X_SITE).set(enable);
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
