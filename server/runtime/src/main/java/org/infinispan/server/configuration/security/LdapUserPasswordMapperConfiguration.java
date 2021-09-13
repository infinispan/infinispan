package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.wildfly.security.auth.realm.ldap.LdapSecurityRealmBuilder;

/**
 * @since 10.0
 */
public class LdapUserPasswordMapperConfiguration extends ConfigurationElement<LdapUserPasswordMapperConfiguration> {
   static final AttributeDefinition<String> FROM = AttributeDefinition.builder(Attribute.FROM, "userPassword", String.class).immutable().build();
   static final AttributeDefinition<Boolean> VERIFIABLE = AttributeDefinition.builder(Attribute.VERIFIABLE, true, Boolean.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(LdapUserPasswordMapperConfiguration.class, FROM, VERIFIABLE);
   }
   LdapUserPasswordMapperConfiguration(AttributeSet attributes) {
      super(Element.USER_PASSWORD_MAPPER, attributes);
   }

   void build(LdapSecurityRealmBuilder ldapRealmBuilder) {
      LdapSecurityRealmBuilder.UserPasswordCredentialLoaderBuilder builder = ldapRealmBuilder.userPasswordCredentialLoader();
      builder.setUserPasswordAttribute(attributes.attribute(FROM).get());
      if (!attributes.attribute(VERIFIABLE).get()) {
         builder.disableVerification();
      }
      builder.build(); // side-effect: adds the credential loader to the ldap realm
   }
}
