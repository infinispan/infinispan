package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.AuthorizationConfiguration.ENABLED;
import static org.infinispan.configuration.cache.AuthorizationConfiguration.ROLES;
import static org.infinispan.util.logging.Log.CONFIG;

import java.util.Set;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * AuthorizationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AuthorizationConfigurationBuilder extends AbstractSecurityConfigurationChildBuilder implements Builder<AuthorizationConfiguration>, ConfigurationBuilderInfo {
   private final AttributeSet attributes;

   public AuthorizationConfigurationBuilder(SecurityConfigurationBuilder securityBuilder) {
      super(securityBuilder);
      attributes = AuthorizationConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return AuthorizationConfiguration.ELEMENT_DEFINITION;
   }

   public AuthorizationConfigurationBuilder disable() {
      attributes.attribute(ENABLED).set(false);
      return this;
   }

   public AuthorizationConfigurationBuilder enable() {
      attributes.attribute(ENABLED).set(true);
      return this;
   }

   public AuthorizationConfigurationBuilder enabled(boolean enabled) {
      this.attributes.attribute(ENABLED).set(enabled);
      return this;
   }


   public AuthorizationConfigurationBuilder role(String name) {
      Set<String> roles = attributes.attribute(ROLES).get();
      roles.add(name);
      attributes.attribute(ROLES).set(roles);
      return this;
   }

   public AuthorizationConfigurationBuilder roles(String... names) {
      for (String name : names) {
         this.role(name);
      }
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      if (attributes.attribute(ENABLED).get() && !globalConfig.security().authorization().enabled())
         throw CONFIG.globalSecurityAuthShouldBeEnabled();
   }

   @Override
   public AuthorizationConfiguration create() {
      return new AuthorizationConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(AuthorizationConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "AuthorizationConfigurationBuilder [attributes=" + attributes + "]";
   }
}
