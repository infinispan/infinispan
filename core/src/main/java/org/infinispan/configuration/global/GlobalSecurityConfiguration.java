package org.infinispan.configuration.global;

/**
 * GlobalSecurityConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GlobalSecurityConfiguration {
   private final GlobalAuthorizationConfiguration authorization;
   private final long securityCacheTimeout;


   GlobalSecurityConfiguration(GlobalAuthorizationConfiguration authorization, long securityCacheTimeout) {
      this.authorization = authorization;
      this.securityCacheTimeout = securityCacheTimeout;
   }

   public GlobalAuthorizationConfiguration authorization() {
      return authorization;
   }

   public long securityCacheTimeout() {
      return securityCacheTimeout;
   }

   @Override
   public String toString() {
      return "GlobalSecurityConfiguration [authorization=" + authorization
            + ", securityCacheTimeout=" + securityCacheTimeout + "]";
   }

}
