package org.infinispan.rest.configuration;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.rest.authentication.Authenticator;

/**
 * AuthenticationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
public class AuthenticationConfiguration implements ConfigurationInfo {
   public static final AttributeDefinition<String> SECURITY_REALM = AttributeDefinition.builder("securityRealm", null, String.class).immutable().build();
   public static final AttributeDefinition<List<String>> MECHANISMS = AttributeDefinition.builder("mechanisms", null, (Class<List<String>>) (Class<?>) List.class).initializer(ArrayList::new).immutable().build();
   public static final AttributeDefinition<Boolean> METRICS_AUTH = AttributeDefinition.builder("metrics-auth", true, Boolean.class).build();

   static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition("authentication");

   private final Boolean enabled;
   private final Attribute<String> securityRealm;
   private final Authenticator authenticator;
   private final Attribute<List<String>> mechanisms;
   private final AttributeSet attributes;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AuthenticationConfiguration.class, MECHANISMS, SECURITY_REALM, METRICS_AUTH);
   }

   AuthenticationConfiguration(AttributeSet attributes, Authenticator authenticator, Boolean enabled) {
      this.attributes = attributes.checkProtection();
      this.enabled = enabled;
      this.mechanisms = attributes.attribute(MECHANISMS);
      this.securityRealm = attributes.attribute(SECURITY_REALM);
      this.authenticator = authenticator;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public boolean enabled() {
      return enabled;
   }

   public List<String> mechanisms() {
      return mechanisms.get();
   }

   public Authenticator authenticator() {
      return authenticator;
   }

   public boolean metricsAuth() {
      return attributes.attribute(METRICS_AUTH).get();
   }

   @Override
   public String toString() {
      return "AuthenticationConfiguration{" +
            "enabled=" + enabled +
            ", securityRealm=" + securityRealm +
            ", authenticator=" + authenticator +
            ", attributes=" + attributes +
            '}';
   }
}
