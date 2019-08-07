package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.wildfly.security.auth.realm.ldap.LdapSecurityRealmBuilder;

/**
 * @since 10.0
 */
public class LdapUserPasswordMapperConfigurationBuilder implements Builder<LdapUserPasswordMapperConfiguration> {
   private final AttributeSet attributes;
   private final LdapSecurityRealmBuilder.UserPasswordCredentialLoaderBuilder credentialLoaderBuilder;

   LdapUserPasswordMapperConfigurationBuilder(LdapRealmConfigurationBuilder ldapConfigurationBuilder) {
      attributes = LdapUserPasswordMapperConfiguration.attributeDefinitionSet();
      this.credentialLoaderBuilder = ldapConfigurationBuilder.getLdapRealmBuilder().userPasswordCredentialLoader();
   }

   public LdapUserPasswordMapperConfigurationBuilder from(String from) {
      attributes.attribute(LdapUserPasswordMapperConfiguration.FROM).set(from);
      credentialLoaderBuilder.setUserPasswordAttribute(from);
      return this;
   }

   public LdapUserPasswordMapperConfigurationBuilder verifiable(boolean verifiable) {
      attributes.attribute(LdapUserPasswordMapperConfiguration.VERIFIABLE).set(verifiable);
      if (!verifiable) {
         credentialLoaderBuilder.disableVerification();
      }
      return this;
   }

   public LdapUserPasswordMapperConfigurationBuilder writable(boolean writable) {
      attributes.attribute(LdapUserPasswordMapperConfiguration.WRITABLE).set(writable);
      if (writable) {
         credentialLoaderBuilder.enablePersistence();
      }
      return this;
   }

   @Override
   public void validate() {
   }

   public void build() {
      credentialLoaderBuilder.build();
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
