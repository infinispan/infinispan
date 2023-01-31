package org.infinispan.server.core.configuration;

/**
 * @since 15.0
 **/
public interface AuthenticationConfiguration {
   String securityRealm();

   boolean enabled();
}
