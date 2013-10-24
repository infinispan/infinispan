package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;

public class VersioningConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<VersioningConfiguration> {

   Boolean enabled;
   VersioningScheme scheme;

   protected VersioningConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   public VersioningConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }

   public VersioningConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   public VersioningConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   public VersioningConfigurationBuilder scheme(VersioningScheme scheme) {
      this.scheme = scheme;
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public VersioningConfiguration create() {
      if (scheme == null)
         scheme = Configurations.isStrictOptimisticTransaction(getBuilder())
               ? VersioningScheme.SIMPLE : VersioningScheme.NONE;

      if (enabled == null)
         enabled = Configurations.isStrictOptimisticTransaction(getBuilder());

      return new VersioningConfiguration(enabled, scheme);
   }

   @Override
   public VersioningConfigurationBuilder read(VersioningConfiguration template) {
      this.enabled = template.enabled();
      this.scheme = template.scheme();

      return this;
   }

   @Override
   public String toString() {
      return "VersioningConfigurationBuilder{" +
            "enabled=" + enabled +
            ", scheme=" + scheme +
            '}';
   }

}
