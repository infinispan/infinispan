package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.JwtConfiguration.AUDIENCE;
import static org.infinispan.server.configuration.security.JwtConfiguration.CLIENT_SSL_CONTEXT;
import static org.infinispan.server.configuration.security.JwtConfiguration.HOST_NAME_VERIFICATION_POLICY;
import static org.infinispan.server.configuration.security.JwtConfiguration.ISSUER;
import static org.infinispan.server.configuration.security.JwtConfiguration.JKU_TIMEOUT;
import static org.infinispan.server.configuration.security.JwtConfiguration.PUBLIC_KEY;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import javax.net.ssl.HostnameVerifier;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.security.HostnameVerificationPolicy;
import org.wildfly.security.auth.realm.token.validator.JwtValidator;

/**
 * @since 10.0
 */
public class JwtConfigurationBuilder implements Builder<JwtConfiguration> {
   private final AttributeSet attributes;
   private final JwtValidator.Builder validatorBuilder = JwtValidator.builder();
   private final RealmConfigurationBuilder realmBuilder;

   JwtConfigurationBuilder(RealmConfigurationBuilder realmBuilder) {
      this.realmBuilder = realmBuilder;
      this.attributes = JwtConfiguration.attributeDefinitionSet();
   }

   boolean isChanged() {
      return this.attributes.isModified();
   }

   public JwtConfigurationBuilder audience(String[] audience) {
      attributes.attribute(AUDIENCE).set(Arrays.asList(audience));
      validatorBuilder.audience(audience);
      return this;
   }

   public JwtConfigurationBuilder clientSSLContext(String value) {
      attributes.attribute(CLIENT_SSL_CONTEXT).set(value);
      validatorBuilder.useSslContext(realmBuilder.realmsBuilder().getSSLContext(value));
      return this;
   }

   public JwtConfigurationBuilder hostNameVerificationPolicy(String value) {
      attributes.attribute(HOST_NAME_VERIFICATION_POLICY).set(value);
      HostnameVerifier verifier = HostnameVerificationPolicy.valueOf(value).getVerifier();
      validatorBuilder.useSslHostnameVerifier(verifier);
      return this;
   }

   public JwtConfigurationBuilder issuers(String[] issuers) {
      attributes.attribute(ISSUER).set(Arrays.asList(issuers));
      validatorBuilder.issuer(issuers);
      return this;
   }

   public JwtConfigurationBuilder jkuTimeout(long timeout) {
      attributes.attribute(JKU_TIMEOUT).set(timeout);
      validatorBuilder.setJkuTimeout(timeout);
      return this;
   }

   public JwtConfigurationBuilder publicKey(String publicKey) {
      attributes.attribute(PUBLIC_KEY).set(publicKey);
      validatorBuilder.publicKey(publicKey.getBytes(StandardCharsets.UTF_8));
      return this;
   }

   JwtValidator.Builder getValidatorBuilder() {
      return validatorBuilder;
   }

   @Override
   public void validate() {
   }

   @Override
   public JwtConfiguration create() {
      return new JwtConfiguration(attributes.protect());
   }

   @Override
   public JwtConfigurationBuilder read(JwtConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }


}
