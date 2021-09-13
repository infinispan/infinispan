package org.infinispan.server.configuration.security;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.function.Supplier;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.security.HostnameVerificationPolicy;
import org.wildfly.security.auth.realm.token.TokenValidator;
import org.wildfly.security.auth.realm.token.validator.OAuth2IntrospectValidator;

/**
 * @since 10.0
 */
public class OAuth2Configuration extends ConfigurationElement<OAuth2Configuration> {

   static final AttributeDefinition<String> CLIENT_ID = AttributeDefinition.builder(Attribute.CLIENT_ID, null, String.class).immutable().build();
   static final AttributeDefinition<Supplier<char[]>> CLIENT_SECRET = AttributeDefinition.builder(Attribute.CLIENT_SECRET, null, (Class<Supplier<char[]>>) (Class<?>) Supplier.class)
         .serializer(AttributeSerializer.SECRET).immutable().build();
   static final AttributeDefinition<String> CLIENT_SSL_CONTEXT = AttributeDefinition.builder(Attribute.CLIENT_SSL_CONTEXT, null, String.class).immutable().build();
   static final AttributeDefinition<String> HOST_VERIFICATION_POLICY = AttributeDefinition.builder(Attribute.HOST_NAME_VERIFICATION_POLICY, null, String.class).immutable().build();
   static final AttributeDefinition<String> INTROSPECTION_URL = AttributeDefinition.builder(Attribute.INTROSPECTION_URL, null, String.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(KeyStoreConfiguration.class, CLIENT_ID, CLIENT_SECRET, CLIENT_SSL_CONTEXT, INTROSPECTION_URL, HOST_VERIFICATION_POLICY);
   }

   OAuth2Configuration(AttributeSet attributes) {
      super(Element.OAUTH2_INTROSPECTION, attributes);
   }

   TokenValidator getValidator(SecurityConfiguration security) {
      OAuth2IntrospectValidator.Builder validatorBuilder = OAuth2IntrospectValidator.builder();
      validatorBuilder.clientId(attributes.attribute(CLIENT_ID).get());
      validatorBuilder.clientSecret(new String(attributes.attribute(CLIENT_SECRET).get().get()));
      try {
         validatorBuilder.tokenIntrospectionUrl(new URL(attributes.attribute(INTROSPECTION_URL).get()));
      } catch (MalformedURLException e) {
         throw Server.log.invalidUrl();
      }
      attributes.attribute(HOST_VERIFICATION_POLICY).apply(v -> validatorBuilder.useSslHostnameVerifier(HostnameVerificationPolicy.valueOf(v).getVerifier()));
      attributes.attribute(CLIENT_SSL_CONTEXT).apply(v -> validatorBuilder.useSslContext(security.realms().getRealm(v).clientSSLContext()));
      return validatorBuilder.build();
   }
}
