package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.TokenRealmConfiguration.AUTH_SERVER_URL;
import static org.infinispan.server.configuration.security.TokenRealmConfiguration.CLIENT_ID;
import static org.infinispan.server.configuration.security.TokenRealmConfiguration.NAME;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class TokenRealmConfigurationBuilder implements RealmProviderBuilder<TokenRealmConfiguration> {
   private final AttributeSet attributes;
   private final JwtConfigurationBuilder jwtConfiguration;
   private final OAuth2ConfigurationBuilder oauth2Configuration;

   public TokenRealmConfigurationBuilder() {
      this.attributes = TokenRealmConfiguration.attributeDefinitionSet();
      this.jwtConfiguration = new JwtConfigurationBuilder();
      this.oauth2Configuration = new OAuth2ConfigurationBuilder();
   }

   public JwtConfigurationBuilder jwtConfiguration() {
      return jwtConfiguration;
   }

   public OAuth2ConfigurationBuilder oauth2Configuration() {
      return oauth2Configuration;
   }

   public TokenRealmConfigurationBuilder name(String name) {
      attributes.attribute(NAME).set(name);
      return this;
   }

   @Override
   public String name() {
      return attributes.attribute(DistributedRealmConfiguration.NAME).get();
   }

   public TokenRealmConfigurationBuilder authServerUrl(String authServerUrl) {
      attributes.attribute(AUTH_SERVER_URL).set(authServerUrl);
      return this;
   }

   public TokenRealmConfigurationBuilder clientId(String clientId) {
      attributes.attribute(CLIENT_ID).set(clientId);
      return this;
   }

   public TokenRealmConfigurationBuilder principalClaim(String principalClaim) {
      attributes.attribute(TokenRealmConfiguration.PRINCIPAL_CLAIM).set(principalClaim);
      return this;
   }

   @Override
   public void validate() {
      jwtConfiguration.validate();
      oauth2Configuration.validate();
      if (oauth2Configuration.isModified() && jwtConfiguration.isModified()) {
         throw new CacheConfigurationException("Cannot have both Oauth2 and JWT as validators");
      }
   }

   @Override
   public TokenRealmConfiguration create() {
      return new TokenRealmConfiguration(jwtConfiguration.create(), oauth2Configuration.create(),
            attributes.protect());
   }

   @Override
   public TokenRealmConfigurationBuilder read(TokenRealmConfiguration template) {
      attributes.read(template.attributes());
      jwtConfiguration.read(template.jwtConfiguration());
      oauth2Configuration.read(template.oauth2Configuration());
      return this;
   }

   @Override
   public int compareTo(RealmProviderBuilder o) {
      return 0; // Irrelevant
   }
}
