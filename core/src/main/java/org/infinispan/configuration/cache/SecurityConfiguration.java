package org.infinispan.configuration.cache;

/**
 * SecurityConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class SecurityConfiguration {

   private final boolean enabled;
   private final AuthorizationConfiguration authorization;

   SecurityConfiguration(AuthorizationConfiguration authorization, boolean enabled) {
      this.authorization = authorization;
      this.enabled = enabled;
   }

   public AuthorizationConfiguration authorization() {
      return authorization;
   }

   public boolean enabled() {
      return enabled;
   }

   @Override
   public String toString() {
      return "SecurityConfiguration [enabled=" + enabled + ", authorization=" + authorization + "]";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((authorization == null) ? 0 : authorization.hashCode());
      result = prime * result + (enabled ? 1231 : 1237);
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
      SecurityConfiguration other = (SecurityConfiguration) obj;
      if (authorization == null) {
         if (other.authorization != null)
            return false;
      } else if (!authorization.equals(other.authorization))
         return false;
      if (enabled != other.enabled)
         return false;
      return true;
   }
}
