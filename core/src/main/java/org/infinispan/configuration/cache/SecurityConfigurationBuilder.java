package org.infinispan.configuration.cache;

import java.util.Collection;
import java.util.Collections;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * SecurityConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class SecurityConfigurationBuilder extends AbstractConfigurationChildBuilder implements SecurityConfigurationChildBuilder, Builder<SecurityConfiguration>, ConfigurationBuilderInfo {
   private final AuthorizationConfigurationBuilder authorizationBuilder;

   public SecurityConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      authorizationBuilder = new AuthorizationConfigurationBuilder(this);
   }

   @Override
   public Collection<ConfigurationBuilderInfo> getChildrenInfo() {
      return Collections.singleton(authorizationBuilder);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return SecurityConfiguration.ELEMENT_DEFINITION;
   }

   @Override
   public void validate() {
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
   public SecurityConfigurationBuilder read(SecurityConfiguration template) {
      this.authorizationBuilder.read(template.authorization());
      return this;
   }

   @Override
   public AuthorizationConfigurationBuilder authorization() {
      return authorizationBuilder;
   }
}
