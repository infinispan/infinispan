package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class TokenRealmConfiguration extends ConfigurationElement<TokenRealmConfiguration> {

   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, null, String.class).build();
   static final AttributeDefinition<String> AUTH_SERVER_URL = AttributeDefinition.builder(Attribute.AUTH_SERVER_URL, null, String.class).build();
   static final AttributeDefinition<String> CLIENT_ID = AttributeDefinition.builder(Attribute.CLIENT_ID, null, String.class).build();
   static final AttributeDefinition<String> PRINCIPAL_CLAIM = AttributeDefinition.builder(Attribute.PRINCIPAL_CLAIM, null, String.class).build();

   private final JwtConfiguration jwtConfiguration;
   private final OAuth2Configuration oAuth2Configuration;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TokenRealmConfiguration.class, NAME, AUTH_SERVER_URL, CLIENT_ID, PRINCIPAL_CLAIM);
   }

   TokenRealmConfiguration(JwtConfiguration jwtConfiguration, OAuth2Configuration oAuth2Configuration, AttributeSet attributes) {
      super(Element.TOKEN_REALM, attributes);
      this.jwtConfiguration = jwtConfiguration;
      this.oAuth2Configuration = oAuth2Configuration;
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
      return oAuth2Configuration;
   }
}
