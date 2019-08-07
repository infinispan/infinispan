package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.Builder;

/**
 * @since 10.0
 */
public class SecurityConfigurationBuilder implements Builder<SecurityConfiguration> {
   private final RealmsConfigurationBuilder realmsConfiguration = new RealmsConfigurationBuilder();

   public RealmsConfigurationBuilder realms() {
      return realmsConfiguration;
   }

   @Override
   public void validate() {
      realmsConfiguration.validate();
   }

   @Override
   public SecurityConfiguration create() {
      return new SecurityConfiguration(realmsConfiguration.create());
   }

   @Override
   public SecurityConfigurationBuilder read(SecurityConfiguration template) {
      realmsConfiguration.read(template.realms());
      return this;
   }
}
