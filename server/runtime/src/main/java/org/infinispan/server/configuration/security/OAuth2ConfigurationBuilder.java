package org.infinispan.server.configuration.security;

import java.net.MalformedURLException;
import java.net.URL;

import javax.net.ssl.HostnameVerifier;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.Server;
import org.infinispan.server.security.HostnameVerificationPolicy;
import org.wildfly.security.auth.realm.token.validator.OAuth2IntrospectValidator;

/**
 * @since 10.0
 */
public class OAuth2ConfigurationBuilder implements Builder<OAuth2Configuration> {
   private final AttributeSet attributes;
   private final OAuth2IntrospectValidator.Builder validatorBuilder = OAuth2IntrospectValidator.builder();
   private final RealmsConfigurationBuilder realms;

   OAuth2ConfigurationBuilder(RealmsConfigurationBuilder realms) {
      this.realms = realms;
      this.attributes = OAuth2Configuration.attributeDefinitionSet();
   }

   boolean isChanged() {
      return this.attributes.isModified();
   }

   public OAuth2ConfigurationBuilder clientId(String clientId) {
      attributes.attribute(OAuth2Configuration.CLIENT_ID).set(clientId);
      validatorBuilder.clientId(clientId);
      return this;
   }

   public OAuth2ConfigurationBuilder clientSecret(char[] clientSecret) {
      attributes.attribute(OAuth2Configuration.CLIENT_SECRET).set(clientSecret);
      validatorBuilder.clientSecret(new String(clientSecret));
      return this;
   }

   public OAuth2ConfigurationBuilder introspectionUrl(String introspectionUrl) {
      attributes.attribute(OAuth2Configuration.INTROSPECTION_URL).set(introspectionUrl);
      try {
         validatorBuilder.tokenIntrospectionUrl(new URL(introspectionUrl));
      } catch (MalformedURLException e) {
         throw Server.log.invalidUrl();
      }
      return this;
   }

   public OAuth2ConfigurationBuilder clientSSLContext(String value) {
      attributes.attribute(OAuth2Configuration.CLIENT_SSL_CONTEXT).set(value);
      validatorBuilder.useSslContext(realms.getSSLContext(value));
      return this;
   }

   public OAuth2ConfigurationBuilder hostVerificationPolicy(String value) {
      attributes.attribute(OAuth2Configuration.HOST_VERIFICATION_POLICY).set(value);
      HostnameVerifier verifier = HostnameVerificationPolicy.valueOf(value).getVerifier();
      validatorBuilder.useSslHostnameVerifier(verifier);
      return this;
   }

   OAuth2IntrospectValidator.Builder getValidatorBuilder() {
      return validatorBuilder;
   }

   @Override
   public void validate() {
   }

   @Override
   public OAuth2Configuration create() {
      return new OAuth2Configuration(attributes.protect());
   }

   @Override
   public OAuth2ConfigurationBuilder read(OAuth2Configuration template) {
      attributes.read(template.attributes());
      return this;
   }
}
