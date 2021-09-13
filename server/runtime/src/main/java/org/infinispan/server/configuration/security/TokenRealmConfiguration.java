package org.infinispan.server.configuration.security;

import java.util.Properties;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.security.KeycloakRoleDecoder;
import org.infinispan.server.security.ServerSecurityRealm;
import org.wildfly.security.auth.realm.token.TokenSecurityRealm;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityRealm;

/**
 * @since 10.0
 */
public class TokenRealmConfiguration extends ConfigurationElement<TokenRealmConfiguration> implements RealmProvider {

   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, "token", String.class).build();
   static final AttributeDefinition<String> AUTH_SERVER_URL = AttributeDefinition.builder(Attribute.AUTH_SERVER_URL, null, String.class).build();
   static final AttributeDefinition<String> CLIENT_ID = AttributeDefinition.builder(Attribute.CLIENT_ID, null, String.class).build();
   static final AttributeDefinition<String> PRINCIPAL_CLAIM = AttributeDefinition.builder(Attribute.PRINCIPAL_CLAIM, null, String.class).build();

   private final JwtConfiguration jwtConfiguration;
   private final OAuth2Configuration oauth2Configuration;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TokenRealmConfiguration.class, NAME, AUTH_SERVER_URL, CLIENT_ID, PRINCIPAL_CLAIM);
   }

   TokenRealmConfiguration(JwtConfiguration jwtConfiguration, OAuth2Configuration oAuth2Configuration, AttributeSet attributes) {
      super(Element.TOKEN_REALM, attributes);
      this.jwtConfiguration = jwtConfiguration;
      this.oauth2Configuration = oAuth2Configuration;
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   public String authServerUrl() {
      return attributes.attribute(AUTH_SERVER_URL).get();
   }

   public String clientId() {
      return attributes.attribute(CLIENT_ID).get();
   }

   public JwtConfiguration jwtConfiguration() {
      return jwtConfiguration;
   }

   public OAuth2Configuration oauth2Configuration() {
      return oauth2Configuration;
   }

   @Override
   public SecurityRealm build(SecurityConfiguration security, RealmConfiguration realm, SecurityDomain.Builder domainBuilder, Properties properties) {
      TokenSecurityRealm.Builder tokenRealmBuilder = TokenSecurityRealm.builder();
      tokenRealmBuilder.validator(oauth2Configuration().isModified() ? oauth2Configuration.getValidator(security) : jwtConfiguration.getValidator(security));
      TokenSecurityRealm securityRealm = tokenRealmBuilder.build();
      realm.addFeature(ServerSecurityRealm.Feature.TOKEN);
      domainBuilder.setRoleDecoder(new KeycloakRoleDecoder());
      return securityRealm;
   }
}
