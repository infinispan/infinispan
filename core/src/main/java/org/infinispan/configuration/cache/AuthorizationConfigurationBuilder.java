package org.infinispan.configuration.cache;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * AuthorizationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AuthorizationConfigurationBuilder extends AbstractSecurityConfigurationChildBuilder implements Builder<AuthorizationConfiguration> {
   private final Set<String> roles = new HashSet<String>();
   private boolean enabled = false;

   public AuthorizationConfigurationBuilder(SecurityConfigurationBuilder securityBuilder) {
      super(securityBuilder);
   }

   public AuthorizationConfigurationBuilder disable() {
      enabled = false;
      return this;
   }

   public AuthorizationConfigurationBuilder enable() {
      enabled = true;
      return this;
   }

   public AuthorizationConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }


   public AuthorizationConfigurationBuilder role(String name) {
      roles.add(name);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public AuthorizationConfiguration create() {
      return new AuthorizationConfiguration(enabled, roles);
   }

   @Override
   public Builder<?> read(AuthorizationConfiguration template) {
      this.enabled = template.enabled();
      this.roles.clear();
      this.roles.addAll(template.roles());

      return this;
   }
}
