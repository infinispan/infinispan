package org.infinispan.rest.configuration;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.rest.authentication.RestAuthenticator;
import org.infinispan.server.core.configuration.AuthenticationConfiguration;

/**
 * AuthenticationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
public class RestAuthenticationConfiguration extends ConfigurationElement<RestAuthenticationConfiguration> implements AuthenticationConfiguration {
   public static final AttributeDefinition<String> SECURITY_REALM = AttributeDefinition.builder("security-realm", null, String.class).immutable().build();
   public static final AttributeDefinition<List<String>> MECHANISMS = AttributeDefinition.builder("mechanisms", null, (Class<List<String>>) (Class<?>) List.class)
         .initializer(ArrayList::new).immutable().serializer(AttributeSerializer.STRING_COLLECTION).build();
   public static final AttributeDefinition<Boolean> METRICS_AUTH = AttributeDefinition.builder("metrics-auth", true, Boolean.class).autoPersist(false).build();

   private final Boolean enabled;
   private final RestAuthenticator authenticator;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RestAuthenticationConfiguration.class, MECHANISMS, SECURITY_REALM, METRICS_AUTH);
   }

   RestAuthenticationConfiguration(AttributeSet attributes, RestAuthenticator authenticator, Boolean enabled) {
      super("authentication", attributes);
      this.enabled = enabled;
      this.authenticator = authenticator;
   }

   public boolean enabled() {
      return enabled;
   }

   public List<String> mechanisms() {
      return attributes.attribute(MECHANISMS).get();
   }

   public RestAuthenticator authenticator() {
      return authenticator;
   }

   public boolean metricsAuth() {
      return attributes.attribute(METRICS_AUTH).get();
   }

   public String securityRealm() {
      return attributes.attribute(SECURITY_REALM).get();
   }
}
