package org.infinispan.configuration.cache;

import java.util.Collections;
import java.util.Set;

/**
 * AuthorizationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AuthorizationConfiguration {
   private final boolean enabled;
   private final Set<String> roles;

   AuthorizationConfiguration(boolean enabled, Set<String> roles) {
      this.enabled = enabled;
      this.roles = Collections.unmodifiableSet(roles);
   }

   public boolean enabled() {
      return enabled;
   }

   public Set<String> roles() {
      return roles;
   }

   @Override
   public String toString() {
      return "AuthorizationConfiguration [enabled=" + enabled + ", roles=" + roles + "]";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (enabled ? 1231 : 1237);
      result = prime * result + ((roles == null) ? 0 : roles.hashCode());
      return result;
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
      if (enabled != other.enabled)
         return false;
      if (roles == null) {
         if (other.roles != null)
            return false;
      } else if (!roles.equals(other.roles))
         return false;
      return true;
   }

}
