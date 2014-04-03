package org.infinispan.configuration.cache;

/**
 * SecurityConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class SecurityConfiguration {

   private final AuthorizationConfiguration authorization;

   SecurityConfiguration(AuthorizationConfiguration authorization) {
      this.authorization = authorization;
   }

   public AuthorizationConfiguration authorization() {
      return authorization;
   }

   @Override
   public String toString() {
      return "SecurityConfiguration [authorization=" + authorization + "]";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((authorization == null) ? 0 : authorization.hashCode());
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
      return true;
   }
}
