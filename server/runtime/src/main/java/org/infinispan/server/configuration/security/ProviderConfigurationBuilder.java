package org.infinispan.server.configuration.security;


import static org.infinispan.server.configuration.security.ProviderConfiguration.CLASS_NAME;
import static org.infinispan.server.configuration.security.ProviderConfiguration.CONFIGURATION;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 14.0
 **/
public class ProviderConfigurationBuilder implements Builder<ProviderConfiguration> {
   private final AttributeSet attributes = ProviderConfiguration.attributeDefinitionSet();

   public ProviderConfigurationBuilder className(String value) {
      attributes.attribute(CLASS_NAME).set(value);
      return this;
   }

   public ProviderConfigurationBuilder configuration(String value) {
      attributes.attribute(CONFIGURATION).set(value);
      return this;
   }

   @Override
   public ProviderConfiguration create() {
      return new ProviderConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(ProviderConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }
}
