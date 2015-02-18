package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.VersioningConfiguration.ENABLED;
import static org.infinispan.configuration.cache.VersioningConfiguration.SCHEME;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

public class VersioningConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<VersioningConfiguration> {

   private final AttributeSet attributes;

   protected VersioningConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = VersioningConfiguration.attributeDefinitionSet();
   }

   public VersioningConfigurationBuilder enable() {
      attributes.attribute(ENABLED).set(true);
      return this;
   }

   public VersioningConfigurationBuilder disable() {
      attributes.attribute(ENABLED).set(false);
      return this;
   }

   public VersioningConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   public VersioningConfigurationBuilder scheme(VersioningScheme scheme) {
      attributes.attribute(SCHEME).set(scheme);
      return this;
   }

   boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   VersioningScheme scheme() {
      return attributes.attribute(SCHEME).get();
   }

   @Override
   public void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public VersioningConfiguration create() {
      return new VersioningConfiguration(attributes.protect());
   }

   @Override
   public VersioningConfigurationBuilder read(VersioningConfiguration template) {
      this.attributes.read(template.attributes());

      return this;
   }

   @Override
   public String toString() {
      return this.getClass().getSimpleName() + attributes;
   }
}
