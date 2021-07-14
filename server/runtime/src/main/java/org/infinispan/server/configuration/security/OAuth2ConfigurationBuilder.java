package org.infinispan.server.configuration.security;

import java.util.function.Supplier;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.InstanceSupplier;

/**
 * @since 10.0
 */
public class OAuth2ConfigurationBuilder implements Builder<OAuth2Configuration> {
   private final AttributeSet attributes;

   OAuth2ConfigurationBuilder() {
      this.attributes = OAuth2Configuration.attributeDefinitionSet();
   }

   boolean isModified() {
      return this.attributes.isModified();
   }

   public OAuth2ConfigurationBuilder clientId(String clientId) {
      attributes.attribute(OAuth2Configuration.CLIENT_ID).set(clientId);
      return this;
   }

   public OAuth2ConfigurationBuilder clientSecret(char[] clientSecret) {
      attributes.attribute(OAuth2Configuration.CLIENT_SECRET).set(new InstanceSupplier<>(clientSecret));
      return this;
   }

   public OAuth2ConfigurationBuilder clientSecret(Supplier<char[]> clientSecret) {
      attributes.attribute(OAuth2Configuration.CLIENT_SECRET).set(clientSecret);
      return this;
   }

   public OAuth2ConfigurationBuilder introspectionUrl(String introspectionUrl) {
      attributes.attribute(OAuth2Configuration.INTROSPECTION_URL).set(introspectionUrl);
      return this;
   }

   public OAuth2ConfigurationBuilder clientSSLContext(String value) {
      attributes.attribute(OAuth2Configuration.CLIENT_SSL_CONTEXT).set(value);
      return this;
   }

   public OAuth2ConfigurationBuilder hostVerificationPolicy(String value) {
      attributes.attribute(OAuth2Configuration.HOST_VERIFICATION_POLICY).set(value);
      return this;
   }

   public OAuth2ConfigurationBuilder connectionTimeout(int timeout) {
      attributes.attribute(OAuth2Configuration.CONNECTION_TIMEOUT).set(timeout);
      return this;
   }

   public OAuth2ConfigurationBuilder readTimeout(int timeout) {
      attributes.attribute(OAuth2Configuration.READ_TIMEOUT).set(timeout);
      return this;
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
