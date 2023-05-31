package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.JwtConfiguration.AUDIENCE;
import static org.infinispan.server.configuration.security.JwtConfiguration.CLIENT_SSL_CONTEXT;
import static org.infinispan.server.configuration.security.JwtConfiguration.CONNECTION_TIMEOUT;
import static org.infinispan.server.configuration.security.JwtConfiguration.HOST_NAME_VERIFICATION_POLICY;
import static org.infinispan.server.configuration.security.JwtConfiguration.ISSUER;
import static org.infinispan.server.configuration.security.JwtConfiguration.JKU_TIMEOUT;
import static org.infinispan.server.configuration.security.JwtConfiguration.PUBLIC_KEY;
import static org.infinispan.server.configuration.security.JwtConfiguration.READ_TIMEOUT;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class JwtConfigurationBuilder implements Builder<JwtConfiguration> {
   private final AttributeSet attributes;

   JwtConfigurationBuilder() {
      this.attributes = JwtConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   boolean isModified() {
      return this.attributes.isModified();
   }

   public JwtConfigurationBuilder audience(String[] audience) {
      attributes.attribute(AUDIENCE).set(audience);
      return this;
   }

   public JwtConfigurationBuilder clientSSLContext(String value) {
      attributes.attribute(CLIENT_SSL_CONTEXT).set(value);
      return this;
   }

   public JwtConfigurationBuilder hostNameVerificationPolicy(String value) {
      attributes.attribute(HOST_NAME_VERIFICATION_POLICY).set(value);
      return this;
   }

   public JwtConfigurationBuilder issuers(String[] issuers) {
      attributes.attribute(ISSUER).set(issuers);
      return this;
   }

   public JwtConfigurationBuilder jkuTimeout(long timeout) {
      attributes.attribute(JKU_TIMEOUT).set(timeout);
      return this;
   }

   public JwtConfigurationBuilder publicKey(String publicKey) {
      attributes.attribute(PUBLIC_KEY).set(publicKey);
      return this;
   }

   public JwtConfigurationBuilder connectionTimeout(int timeout) {
      attributes.attribute(CONNECTION_TIMEOUT).set(timeout);
      return this;
   }

   public JwtConfigurationBuilder readTimeout(int timeout) {
      attributes.attribute(READ_TIMEOUT).set(timeout);
      return this;
   }

   @Override
   public JwtConfiguration create() {
      return new JwtConfiguration(attributes.protect());
   }

   @Override
   public JwtConfigurationBuilder read(JwtConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }
}
