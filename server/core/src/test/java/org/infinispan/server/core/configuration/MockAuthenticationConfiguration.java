package org.infinispan.server.core.configuration;

/**
 * @since 15.0
 **/
public class MockAuthenticationConfiguration implements AuthenticationConfiguration {
   @Override
   public String securityRealm() {
      return "default";
   }

   @Override
   public boolean enabled() {
      return false;
   }
}
