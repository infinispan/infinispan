package org.infinispan.rest.configuration;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.rest.authentication.Authenticator;

/**
 * AuthenticationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
public class AuthenticationConfiguration {
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   public static final AttributeDefinition<Authenticator> AUTHENTICATOR = AttributeDefinition.builder("authenticator", null, Authenticator.class).immutable().build();
   public static final AttributeDefinition<List<String>> MECHANISMS = AttributeDefinition.builder("mechanisms", null, (Class<List<String>>) (Class<?>) List.class).initializer(ArrayList::new).immutable().build();

   private final Attribute<Boolean> enabled;
   private final Attribute<Authenticator> authenticator;
   private final Attribute<List<String>> mechanisms;
   private final AttributeSet attributes;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AuthenticationConfiguration.class, ENABLED, AUTHENTICATOR, MECHANISMS);
   }

   AuthenticationConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.enabled = attributes.attribute(ENABLED);
      this.authenticator = attributes.attribute(AUTHENTICATOR);
      this.mechanisms = attributes.attribute(MECHANISMS);

   }

   public AttributeSet attributes() {
      return attributes;
   }

   public boolean enabled() {
      return enabled.get();
   }

   public List<String> mechanisms() {
      return mechanisms.get();
   }

   public Authenticator authenticator() {
      return authenticator.get();
   }

   @Override
   public String toString() {
      return "AuthenticationConfiguration[" +
            "enabled=" + enabled +
            ", mechanisms=" + mechanisms +
            ", authenticator=" + authenticator +
            ']';
   }
}
