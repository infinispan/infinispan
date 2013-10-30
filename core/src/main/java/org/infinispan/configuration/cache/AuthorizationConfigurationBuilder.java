package org.infinispan.configuration.cache;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.configuration.Builder;

/**
 * AuthorizationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AuthorizationConfigurationBuilder extends AbstractSecurityConfigurationChildBuilder implements Builder<AuthorizationConfiguration> {
   private Set<String> roles = new HashSet<String>();

   public AuthorizationConfigurationBuilder(SecurityConfigurationBuilder securityBuilder) {
      super(securityBuilder);
   }

   public AuthorizationConfigurationBuilder role(String name) {
      roles.add(name);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public AuthorizationConfiguration create() {
      return new AuthorizationConfiguration(roles);
   }

   @Override
   public Builder<?> read(AuthorizationConfiguration template) {
      this.roles.clear();
      this.roles.addAll(template.roles());

      return this;
   }

}
