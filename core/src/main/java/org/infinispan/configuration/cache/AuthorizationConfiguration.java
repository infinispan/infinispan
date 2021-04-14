package org.infinispan.configuration.cache;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;

/**
 * AuthorizationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AuthorizationConfiguration extends ConfigurationElement<AuthorizationConfiguration> {
   public static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ENABLED, false).immutable().build();
   public static final AttributeDefinition<Set> ROLES = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ROLES, null, Set.class).initializer(HashSet::new).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AuthorizationConfiguration.class, ENABLED, ROLES);
   }

   private final Attribute<Boolean> enabled;
   private final Attribute<Set> roles;

   AuthorizationConfiguration(AttributeSet attributes) {
      super(Element.AUTHORIZATION, attributes);
      enabled = attributes.attribute(ENABLED);
      roles = attributes.attribute(ROLES);
   }

   public boolean enabled() {
      return enabled.get();
   }

   public Set<String> roles() {
      return roles.get();
   }
}
