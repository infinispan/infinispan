package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.TokenRealmConfiguration.AUTH_SERVER_URL;
import static org.infinispan.server.configuration.security.TokenRealmConfiguration.CLIENT_ID;
import static org.infinispan.server.configuration.security.TokenRealmConfiguration.NAME;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.security.KeycloakRoleDecoder;
import org.infinispan.server.security.ServerSecurityRealm;
import org.wildfly.security.auth.realm.token.TokenSecurityRealm;
import org.wildfly.security.auth.realm.token.TokenValidator;
import org.wildfly.security.auth.realm.token.validator.JwtValidator;
import org.wildfly.security.auth.realm.token.validator.OAuth2IntrospectValidator;

/**
 * @since 10.0
 */
public class TokenRealmConfigurationBuilder implements Builder<TokenRealmConfiguration> {
   private final AttributeSet attributes;
   private final TokenSecurityRealm.Builder tokenRealmBuilder = TokenSecurityRealm.builder();
   private final JwtConfigurationBuilder jwtConfiguration;
   private final OAuth2ConfigurationBuilder oauth2Configuration;
   private final RealmConfigurationBuilder realmBuilder;

   private TokenSecurityRealm securityRealm;

   public TokenRealmConfigurationBuilder(RealmConfigurationBuilder realmBuilder) {
      this.realmBuilder = realmBuilder;
      this.attributes = TokenRealmConfiguration.attributeDefinitionSet();
      this.jwtConfiguration = new JwtConfigurationBuilder(realmBuilder);
      this.oauth2Configuration = new OAuth2ConfigurationBuilder(realmBuilder.realmsBuilder());
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

   public TokenSecurityRealm build() {
      if (securityRealm == null) {
         if (oauth2Configuration.isChanged() && jwtConfiguration.isChanged()) {
            throw new CacheConfigurationException("Cannot have both Oauth2 and JWT as validators");
         }
         if (!oauth2Configuration.isChanged() && !jwtConfiguration.isChanged()) {
            return null;
         }
         String name = attributes.attribute(NAME).get();
         TokenValidator validator;
         if (oauth2Configuration.isChanged()) {
            OAuth2IntrospectValidator.Builder oauthValidatorBuilder = oauth2Configuration.getValidatorBuilder();
            validator = oauthValidatorBuilder.build();
         } else {
            JwtValidator.Builder jwtValidatorBuilder = jwtConfiguration.getValidatorBuilder();
            validator = jwtValidatorBuilder.build();
         }
         tokenRealmBuilder.validator(validator);
         securityRealm = tokenRealmBuilder.build();
         realmBuilder.addRealm(name, securityRealm, b -> b.setRoleDecoder(new KeycloakRoleDecoder()));
         realmBuilder.addFeature(ServerSecurityRealm.Feature.TOKEN);
      }
      return securityRealm;
   }

   @Override
   public void validate() {
      jwtConfiguration.validate();
      oauth2Configuration.validate();
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
}
