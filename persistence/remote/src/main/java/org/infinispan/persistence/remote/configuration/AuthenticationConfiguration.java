package org.infinispan.persistence.remote.configuration;

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.Util;

/**
 * AuthenticationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class AuthenticationConfiguration {
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false, Boolean.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> USERNAME = AttributeDefinition.builder("username", null, String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> PASSWORD = AttributeDefinition.builder("password", null, String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> REALM = AttributeDefinition.builder("realm", null, String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<CallbackHandler> CALLBACK_HANDLER = AttributeDefinition.builder("callback-handler", null, CallbackHandler.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<Subject> CLIENT_SUBJECT = AttributeDefinition.builder("client-subject", null, Subject.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> SASL_MECHANISM = AttributeDefinition.builder("sasl-mechanism", null, String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> SERVER_NAME = AttributeDefinition.builder("server-name", null, String.class).immutable().build();
   static final AttributeDefinition<Map> SASL_PROPERTIES = AttributeDefinition.builder("sasl-properties", null, Map.class)
         .initializer(() -> new HashMap()).autoPersist(false).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AuthenticationConfiguration.class, ENABLED, USERNAME, PASSWORD, REALM, CALLBACK_HANDLER, CLIENT_SUBJECT,
            SASL_MECHANISM, SERVER_NAME, SASL_PROPERTIES);
   }

   private final AttributeSet attributes;

   public AuthenticationConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public CallbackHandler callbackHandler() {
      return attributes.attribute(CALLBACK_HANDLER).get();
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   public String username() {
      return attributes.attribute(USERNAME).get();
   }

   public char[] password() {
      return Util.toCharArray(attributes.attribute(PASSWORD).get());
   }

   public String realm() {
      return attributes.attribute(REALM).get();
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

   @Override
   public String toString() {
      return attributes.toString();
   }

   @Override
   public boolean equals(Object o) {
      AuthenticationConfiguration other = (AuthenticationConfiguration) o;
      return attributes.equals(other.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }
}
