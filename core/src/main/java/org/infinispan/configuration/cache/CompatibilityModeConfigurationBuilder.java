package org.infinispan.configuration.cache;

import org.infinispan.configuration.Builder;
import org.infinispan.marshall.Marshaller;

/**
 * Compatibility mode configuration builder
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class CompatibilityModeConfigurationBuilder
      extends AbstractConfigurationChildBuilder implements Builder<CompatibilityModeConfiguration> {

   private boolean enabled;
   private Marshaller marshaller;

   CompatibilityModeConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Enables compatibility mode between embedded and different remote
    * endpoints (Hot Rod, Memcached, REST...etc).
    */
   public CompatibilityModeConfigurationBuilder enable() {
      enabled = true;
      return this;
   }

   /**
    * Disables compatibility mode between embedded.
    */
   public CompatibilityModeConfigurationBuilder disable() {
      enabled = false;
      return this;
   }

   /**
    * Sets whether compatibility mode is enabled or disabled.
    *
    * @param enabled if true, compatibility mode is enabled.  If false, it is disabled.
    */
   public CompatibilityModeConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   /**
    * Sets the marshaller instance to be used by the interoperability layer.
    */
   public CompatibilityModeConfigurationBuilder marshaller(Marshaller marshaller) {
      this.marshaller = marshaller;
      return this;
   }

   @Override
   public void validate() {
      // No-op
   }

   @Override
   public CompatibilityModeConfiguration create() {
      return new CompatibilityModeConfiguration(enabled, marshaller);
   }

   @Override
   public Builder<?> read(CompatibilityModeConfiguration template) {
      this.enabled = template.enabled();
      this.marshaller = template.marshaller();
      return this;
   }

}
