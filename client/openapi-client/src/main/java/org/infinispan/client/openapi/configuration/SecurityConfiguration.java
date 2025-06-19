package org.infinispan.client.openapi.configuration;

/**
 * SecurityConfiguration.
 *
 * @author Tristan Tarrant
 * @since 16.0
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

   @Override
   public String toString() {
      return "SecurityConfiguration{" +
            "authentication=" + authentication +
            ", ssl=" + ssl +
            '}';
   }
}
