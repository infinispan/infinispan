package org.infinispan.persistence.remote.configuration;

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
 * @author Tristan Tarrant
 * @since 9.1
 */
public class AuthenticationConfiguration extends ConfigurationElement<AuthenticationConfiguration> {
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder(Attribute.ENABLED, false, Boolean.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<CallbackHandler> CALLBACK_HANDLER = AttributeDefinition.builder("callback-handler", null, CallbackHandler.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<Subject> CLIENT_SUBJECT = AttributeDefinition.builder("client-subject", null, Subject.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> SERVER_NAME = AttributeDefinition.builder(Attribute.SERVER_NAME, null, String.class).immutable().build();
   static final AttributeDefinition<Map> SASL_PROPERTIES = AttributeDefinition.builder(Element.PROPERTIES, null, Map.class).initializer(HashMap::new).autoPersist(false).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AuthenticationConfiguration.class, ENABLED, CALLBACK_HANDLER, CLIENT_SUBJECT, SERVER_NAME, SASL_PROPERTIES);
   }

   private final MechanismConfiguration mechanismConfiguration;

   public AuthenticationConfiguration(AttributeSet attributes, MechanismConfiguration mechanismConfiguration) {
      super(Element.AUTHENTICATION, attributes);
      this.mechanismConfiguration = mechanismConfiguration;
   }

   MechanismConfiguration mechanismConfiguration() {
      return mechanismConfiguration;
   }

   public CallbackHandler callbackHandler() {
      return attributes.attribute(CALLBACK_HANDLER).get();
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   public String username() {
      return mechanismConfiguration.username();
   }

   public char[] password() {
      return mechanismConfiguration.password();
   }

   public String realm() {
      return mechanismConfiguration.realm();
   }

   public String saslMechanism() {
      return mechanismConfiguration.saslMechanism();
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
