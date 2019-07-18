package org.infinispan.client.rest.configuration;

/**
 * SecurityConfiguration.
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
public class SecurityConfiguration {

   private final AuthenticationConfiguration authentication;
   private final SslConfiguration ssl;

   SecurityConfiguration(AuthenticationConfiguration authentication, SslConfiguration ssl) {
      this.authentication = authentication;
      this.ssl = ssl;
   }

   public AuthenticationConfiguration authentication() {
      return authentication;
   }

   public SslConfiguration ssl() {
      return ssl;
   }

}
