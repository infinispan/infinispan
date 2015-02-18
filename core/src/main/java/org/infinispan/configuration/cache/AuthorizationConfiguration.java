package org.infinispan.configuration.cache;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeInitializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * AuthorizationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AuthorizationConfiguration {
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   static final AttributeDefinition<Set> ROLES = AttributeDefinition.builder("roles", null, Set.class).initializer(new AttributeInitializer<Set>() {
      @Override
      public Set initialize() {
         return new HashSet<String>();
      }
   }).build();
   static final AttributeSet attributeSet() {
      return new AttributeSet(AuthorizationConfiguration.class, ENABLED, ROLES);
   }
   private final AttributeSet attributes;

   AuthorizationConfiguration(AttributeSet attributes) {
      attributes.checkProtection();
      this.attributes = attributes;
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).asBoolean();
   }

   public Set<String> roles() {
      return attributes.attribute(ROLES).asObject(Set.class);
   }

   @Override
   public String toString() {
      return "AuthorizationConfiguration [attributes=" + attributes + "]";
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      AuthorizationConfiguration other = (AuthorizationConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

   AttributeSet attributes() {
      return attributes;
   }

}
