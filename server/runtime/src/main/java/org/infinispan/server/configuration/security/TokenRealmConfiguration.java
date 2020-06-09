package org.infinispan.server.configuration.security;

import java.util.Arrays;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class TokenRealmConfiguration implements ConfigurationInfo {

   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();
   static final AttributeDefinition<String> AUTH_SERVER_URL = AttributeDefinition.builder("authServerUrl", null, String.class).build();
   static final AttributeDefinition<String> CLIENT_ID = AttributeDefinition.builder("clientId", null, String.class).build();
   static final AttributeDefinition<String> PRINCIPAL_CLAIM = AttributeDefinition.builder("principalClaim", null, String.class).build();

   private final JwtConfiguration jwtConfiguration;
   private final OAuth2Configuration oAuth2Configuration;
   private final List<ConfigurationInfo> elements;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TokenRealmConfiguration.class, NAME, AUTH_SERVER_URL, CLIENT_ID, PRINCIPAL_CLAIM);
   }

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.TOKEN_REALM.toString());
   private final AttributeSet attributes;

   TokenRealmConfiguration(JwtConfiguration jwtConfiguration, OAuth2Configuration oAuth2Configuration, AttributeSet attributes) {
      this.jwtConfiguration = jwtConfiguration;
      this.oAuth2Configuration = oAuth2Configuration;
      this.attributes = attributes.checkProtection();
      this.elements = Arrays.asList(jwtConfiguration, oAuth2Configuration);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return elements;
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

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

}
