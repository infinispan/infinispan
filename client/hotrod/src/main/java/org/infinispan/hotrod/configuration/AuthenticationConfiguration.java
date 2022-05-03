package org.infinispan.hotrod.configuration;

import static org.infinispan.commons.configuration.attributes.CollectionAttributeCopier.collectionCopier;

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

/**
 * AuthenticationConfiguration.
 *
 * @since 14.0
 */
public class AuthenticationConfiguration extends ConfigurationElement<AuthenticationConfiguration> {
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("use-auth", false, Boolean.class).build();
   static final AttributeDefinition<CallbackHandler> CALLBACK_HANDLER = AttributeDefinition.builder("callback-handler", null, CallbackHandler.class).build();
   static final AttributeDefinition<Subject> CLIENT_SUBJECT = AttributeDefinition.builder("client-subject", null, Subject.class).build();
   static final AttributeDefinition<String> SASL_MECHANISM = AttributeDefinition.builder("sasl-mechanism", "SCRAM-SHA-512", String.class).build();
   static final AttributeDefinition<Map<String, String>> SASL_PROPERTIES = AttributeDefinition.builder("sasl-properties", null, (Class<Map<String, String>>) (Class<?>) Map.class)
         .copier(collectionCopier())
         .initializer(HashMap::new).immutable().build();
   static final AttributeDefinition<String> SERVER_NAME = AttributeDefinition.builder("server-name", "infinispan", String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AuthenticationConfiguration.class, ENABLED, CALLBACK_HANDLER, CLIENT_SUBJECT, SASL_MECHANISM, SASL_PROPERTIES, SERVER_NAME);
   }

   AuthenticationConfiguration(AttributeSet attributes) {
      super("authentication", attributes);
   }

   public CallbackHandler callbackHandler() {
      return attributes.attribute(CALLBACK_HANDLER).get();
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   public String saslMechanism() {
      return attributes.attribute(SASL_MECHANISM).get();
   }

   public Map<String, String> saslProperties() {
      return attributes.attribute(SASL_PROPERTIES).get();
   }

   public String serverName() {
      return attributes.attribute(SERVER_NAME).get();
   }

   public Subject clientSubject() {
      return attributes.attribute(CLIENT_SUBJECT).get();
   }
}
