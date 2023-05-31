package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * SecurityConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class SecurityConfigurationBuilder extends AbstractConfigurationChildBuilder implements SecurityConfigurationChildBuilder, Builder<SecurityConfiguration> {
   private final AuthorizationConfigurationBuilder authorizationBuilder;

   public SecurityConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      authorizationBuilder = new AuthorizationConfigurationBuilder(this);
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
   }


   @Override
   public void validate(GlobalConfiguration globalConfig) {
      authorizationBuilder.validate(globalConfig);
   }

   @Override
   public SecurityConfiguration create() {
      return new SecurityConfiguration(authorizationBuilder.create());
   }

   @Override
   public SecurityConfigurationBuilder read(SecurityConfiguration template, Combine combine) {
      this.authorizationBuilder.read(template.authorization(), combine);
      return this;
   }

   @Override
   public AuthorizationConfigurationBuilder authorization() {
      return authorizationBuilder;
   }
}
