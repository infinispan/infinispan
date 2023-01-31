package org.infinispan.server.core.configuration;

/**
 * @since 15.0
 **/
public class NoAuthenticationConfiguration implements AuthenticationConfiguration {
   @Override
   public String securityRealm() {
      return null;
   }

   @Override
   public boolean enabled() {
      return false;
   }
}
