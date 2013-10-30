package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;

/**
 * SecurityConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class SecurityConfigurationBuilder extends AbstractConfigurationChildBuilder implements SecurityConfigurationChildBuilder, Builder<SecurityConfiguration> {
   private AuthorizationConfigurationBuilder authorizationBuilder;
   private boolean enabled = false;

   public SecurityConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      authorizationBuilder = new AuthorizationConfigurationBuilder(this);
   }

   public SecurityConfigurationBuilder disable() {
      enabled = false;
      return this;
   }

   public SecurityConfigurationBuilder enable() {
      enabled = true;
      return this;
   }

   public SecurityConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public SecurityConfiguration create() {
      return new SecurityConfiguration(authorizationBuilder.create(), enabled);
   }

   @Override
   public SecurityConfigurationBuilder read(SecurityConfiguration template) {
      this.enabled = template.enabled();
      this.authorizationBuilder.read(template.authorization());
      return this;
   }

   @Override
   public AuthorizationConfigurationBuilder authorization() {
      return authorizationBuilder;
   }

}
