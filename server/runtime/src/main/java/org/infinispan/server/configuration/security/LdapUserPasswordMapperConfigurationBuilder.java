package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class LdapUserPasswordMapperConfigurationBuilder implements Builder<LdapUserPasswordMapperConfiguration> {
   private final AttributeSet attributes;

   LdapUserPasswordMapperConfigurationBuilder() {
      attributes = LdapUserPasswordMapperConfiguration.attributeDefinitionSet();
   }

   public LdapUserPasswordMapperConfigurationBuilder from(String from) {
      attributes.attribute(LdapUserPasswordMapperConfiguration.FROM).set(from);
      return this;
   }

   public LdapUserPasswordMapperConfigurationBuilder verifiable(boolean verifiable) {
      attributes.attribute(LdapUserPasswordMapperConfiguration.VERIFIABLE).set(verifiable);
      return this;
   }

   @Override
   public LdapUserPasswordMapperConfiguration create() {
      return new LdapUserPasswordMapperConfiguration(attributes.protect());
   }

   @Override
   public LdapUserPasswordMapperConfigurationBuilder read(LdapUserPasswordMapperConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
