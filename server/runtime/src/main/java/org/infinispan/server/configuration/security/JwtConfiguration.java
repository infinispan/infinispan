package org.infinispan.server.configuration.security;

import java.nio.charset.StandardCharsets;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.security.HostnameVerificationPolicy;
import org.wildfly.security.auth.realm.token.TokenValidator;
import org.wildfly.security.auth.realm.token.validator.JwtValidator;

/**
 * @since 10.0
 */
public class JwtConfiguration extends ConfigurationElement<JwtConfiguration> {
   static final AttributeDefinition<String[]> AUDIENCE = AttributeDefinition.builder(Attribute.AUDIENCE, null, String[].class).build();
   static final AttributeDefinition<String> CLIENT_SSL_CONTEXT = AttributeDefinition.builder(Attribute.CLIENT_SSL_CONTEXT, null, String.class).build();
   static final AttributeDefinition<String> HOST_NAME_VERIFICATION_POLICY = AttributeDefinition.builder(Attribute.HOST_NAME_VERIFICATION_POLICY, null, String.class).build();
   static final AttributeDefinition<String[]> ISSUER = AttributeDefinition.builder(Attribute.ISSUER, null, String[].class).build();
   static final AttributeDefinition<Long> JKU_TIMEOUT = AttributeDefinition.builder(Attribute.JKU_TIMEOUT, null, Long.class).build();
   static final AttributeDefinition<String> PUBLIC_KEY = AttributeDefinition.builder(Attribute.PUBLIC_KEY, null, String.class).build();
   static final AttributeDefinition<Integer> CONNECTION_TIMEOUT = AttributeDefinition.builder(Attribute.CONNECTION_TIMEOUT, 2000, Integer.class).immutable().build();
   static final AttributeDefinition<Integer> READ_TIMEOUT = AttributeDefinition.builder(Attribute.READ_TIMEOUT, 2000, Integer.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(JwtConfiguration.class, AUDIENCE, CLIENT_SSL_CONTEXT, HOST_NAME_VERIFICATION_POLICY, ISSUER, JKU_TIMEOUT, PUBLIC_KEY, CONNECTION_TIMEOUT, READ_TIMEOUT);
   }

   JwtConfiguration(AttributeSet attributes) {
      super(Element.JWT, attributes);
   }

   public TokenValidator getValidator(SecurityConfiguration security, RealmConfiguration realm) {
      JwtValidator.Builder validatorBuilder = JwtValidator.builder();
      attributes.attribute(AUDIENCE).apply(v -> validatorBuilder.audience(v));
      attributes.attribute(ISSUER).apply(v -> validatorBuilder.issuer(v));
      attributes.attribute(JKU_TIMEOUT).apply(v -> validatorBuilder.setJkuTimeout(v));
      attributes.attribute(PUBLIC_KEY).apply(v -> validatorBuilder.publicKey(v.getBytes(StandardCharsets.UTF_8)));
      attributes.attribute(HOST_NAME_VERIFICATION_POLICY).apply(v -> validatorBuilder.useSslHostnameVerifier(HostnameVerificationPolicy.valueOf(v).getVerifier()));
      attributes.attribute(CONNECTION_TIMEOUT).apply(v -> validatorBuilder.connectionTimeout(v));
      attributes.attribute(READ_TIMEOUT).apply(v -> validatorBuilder.readTimeout(v));
      RealmConfiguration sslRealm = attributes.attribute(CLIENT_SSL_CONTEXT).isNull() ? realm : security.realms().getRealm(attributes.attribute(CLIENT_SSL_CONTEXT).get());
      validatorBuilder.useSslContext(sslRealm.clientSSLContext());
      return validatorBuilder.build();
   }
}
