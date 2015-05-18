package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.CompatibilityModeConfiguration.ENABLED;
import static org.infinispan.configuration.cache.CompatibilityModeConfiguration.MARSHALLER;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.global.GlobalConfiguration;
/**
 * Compatibility mode configuration builder
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class CompatibilityModeConfigurationBuilder
      extends AbstractConfigurationChildBuilder implements Builder<CompatibilityModeConfiguration> {

   private final AttributeSet attributes;

   CompatibilityModeConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = CompatibilityModeConfiguration.attributeDefinitionSet();
   }

   /**
    * Enables compatibility mode between embedded and different remote
    * endpoints (Hot Rod, Memcached, REST...etc).
    */
   public CompatibilityModeConfigurationBuilder enable() {
      attributes.attribute(ENABLED).set(true);
      return this;
   }

   /**
    * Disables compatibility mode between embedded.
    */
   public CompatibilityModeConfigurationBuilder disable() {
      attributes.attribute(ENABLED).set(false);
      return this;
   }

   /**
    * Sets whether compatibility mode is enabled or disabled.
    *
    * @param enabled if true, compatibility mode is enabled.  If false, it is disabled.
    */
   public CompatibilityModeConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   /**
    * Sets the marshaller instance to be used by the interoperability layer.
    */
   public CompatibilityModeConfigurationBuilder marshaller(Marshaller marshaller) {
      attributes.attribute(MARSHALLER).set(marshaller);
      return this;
   }

   public Marshaller marshaller() {
      return attributes.attribute(MARSHALLER).get();
   }

   @Override
   public void validate() {
      // No-op
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public CompatibilityModeConfiguration create() {
      return new CompatibilityModeConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(CompatibilityModeConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
