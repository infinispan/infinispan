package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.CredentialStoresConfiguration.resolvePassword;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.function.Supplier;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.configuration.ServerConfigurationSerializer;
import org.infinispan.server.security.HostnameVerificationPolicy;
import org.wildfly.security.auth.realm.token.TokenValidator;
import org.wildfly.security.auth.realm.token.validator.OAuth2IntrospectValidator;
import org.wildfly.security.credential.source.CredentialSource;

/**
 * @since 10.0
 */
public class OAuth2Configuration extends ConfigurationElement<OAuth2Configuration> {

   static final AttributeDefinition<String> CLIENT_ID = AttributeDefinition.builder(Attribute.CLIENT_ID, null, String.class).immutable().build();
   static final AttributeDefinition<Supplier<CredentialSource>> CLIENT_SECRET = AttributeDefinition.builder(Attribute.CLIENT_SECRET, null, (Class<Supplier<CredentialSource>>) (Class<?>) Supplier.class)
         .serializer(ServerConfigurationSerializer.CREDENTIAL).immutable().build();
   static final AttributeDefinition<String> CLIENT_SSL_CONTEXT = AttributeDefinition.builder(Attribute.CLIENT_SSL_CONTEXT, null, String.class).immutable().build();
   static final AttributeDefinition<String> HOST_VERIFICATION_POLICY = AttributeDefinition.builder(Attribute.HOST_NAME_VERIFICATION_POLICY, null, String.class).immutable().build();
   static final AttributeDefinition<String> INTROSPECTION_URL = AttributeDefinition.builder(Attribute.INTROSPECTION_URL, null, String.class).immutable().build();
   static final AttributeDefinition<Integer> CONNECTION_TIMEOUT = AttributeDefinition.builder(Attribute.CONNECTION_TIMEOUT, 2000, Integer.class).immutable().build();
   static final AttributeDefinition<Integer> READ_TIMEOUT = AttributeDefinition.builder(Attribute.READ_TIMEOUT, 2000, Integer.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(OAuth2Configuration.class, CLIENT_ID, CLIENT_SSL_CONTEXT, INTROSPECTION_URL, HOST_VERIFICATION_POLICY, CONNECTION_TIMEOUT, READ_TIMEOUT, CLIENT_SECRET);
   }

   OAuth2Configuration(AttributeSet attributes) {
      super(Element.OAUTH2_INTROSPECTION, attributes);
   }

   TokenValidator getValidator(SecurityConfiguration security, RealmConfiguration realm) {
      OAuth2IntrospectValidator.Builder validatorBuilder = OAuth2IntrospectValidator.builder();
      validatorBuilder.clientId(attributes.attribute(CLIENT_ID).get());
      validatorBuilder.clientSecret(new String(resolvePassword(attributes.attribute(CLIENT_SECRET))));
      final URL url;
      try {
         url = new URI(attributes.attribute(INTROSPECTION_URL).get()).toURL();
         validatorBuilder.tokenIntrospectionUrl(url);
      } catch (URISyntaxException | MalformedURLException e) {
         throw Server.log.invalidUrl(attributes.attribute(INTROSPECTION_URL).get());
      }
      if ("https".equalsIgnoreCase(url.getProtocol())) {
         RealmConfiguration sslRealm = attributes.attribute(CLIENT_SSL_CONTEXT).isNull() ? realm : security.realms().getRealm(attributes.attribute(CLIENT_SSL_CONTEXT).get());
         validatorBuilder.useSslContext(sslRealm.clientSSLContext());
         if (!attributes.attribute(HOST_VERIFICATION_POLICY).isNull()) {
            validatorBuilder.useSslHostnameVerifier(HostnameVerificationPolicy.valueOf(attributes.attribute(HOST_VERIFICATION_POLICY).get()).getVerifier());
         }
      }
      return validatorBuilder.build();
   }
}
