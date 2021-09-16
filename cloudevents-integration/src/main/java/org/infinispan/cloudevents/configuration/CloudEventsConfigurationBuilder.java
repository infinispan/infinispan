package org.infinispan.cloudevents.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.Experimental;
import org.infinispan.configuration.cache.ConfigurationBuilder;

/**
 * Configuration module builder to control the CloudEvents integration for a cache.
 *
 * @see CloudEventsConfiguration
 *
 * @since 12
 * @author Dan Berindei
 */
@Experimental
public class CloudEventsConfigurationBuilder implements Builder<CloudEventsConfiguration> {
   private final AttributeSet attributes;
   private final ConfigurationBuilder rootBuilder;

   public CloudEventsConfigurationBuilder(ConfigurationBuilder builder) {
      rootBuilder = builder;
      this.attributes = CloudEventsConfiguration.attributeSet();
   }

   /**
    * Enable or disable anchored keys.
    */
   public void enabled(boolean enabled) {
      attributes.attribute(CloudEventsConfiguration.ENABLED).set(enabled);
   }

   @Override
   public CloudEventsConfiguration create() {
      return new CloudEventsConfiguration(attributes);
   }

   @Override
   public Builder<?> read(CloudEventsConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
