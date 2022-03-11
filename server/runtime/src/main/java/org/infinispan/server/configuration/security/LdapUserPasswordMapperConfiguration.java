package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.security.ServerSecurityRealm;
import org.wildfly.security.auth.realm.ldap.LdapSecurityRealmBuilder;

/**
 * @since 10.0
 */
public class LdapUserPasswordMapperConfiguration extends ConfigurationElement<LdapUserPasswordMapperConfiguration> {
   static final AttributeDefinition<String> FROM = AttributeDefinition.builder(Attribute.FROM, null, String.class).immutable().build();
   static final AttributeDefinition<Boolean> VERIFIABLE = AttributeDefinition.builder(Attribute.VERIFIABLE, true, Boolean.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(LdapUserPasswordMapperConfiguration.class, FROM, VERIFIABLE);
   }
   LdapUserPasswordMapperConfiguration(AttributeSet attributes) {
      super(Element.USER_PASSWORD_MAPPER, attributes);
   }

   void build(LdapSecurityRealmBuilder ldapRealmBuilder, RealmConfiguration realm) {
      if (attributes.attribute(FROM).get() != null) {
         LdapSecurityRealmBuilder.UserPasswordCredentialLoaderBuilder builder = ldapRealmBuilder.userPasswordCredentialLoader();
         builder.setUserPasswordAttribute(attributes.attribute(FROM).get());
         if (!attributes.attribute(VERIFIABLE).get()) {
            builder.disableVerification();
         } else {
            /*
             * At this stage, we can only guess that the user password attribute can be used for hashed password verification.
             * The only way to verify this would be to attempt connecting to the LDAP server using the configured credentials,
             * fetch the user password attribute and see if it is prefixed with one of the known hash names.
             *
             * See https://issues.redhat.com/browse/ELY-296
             */
            realm.addFeature(ServerSecurityRealm.Feature.PASSWORD_HASHED);
         }
         builder.build(); // side-effect: adds the credential loader to the ldap realm
      }
   }
}
