package org.infinispan.persistence.remote.configuration;

/**
 * SecurityConfiguration.
 *
 * @author Tristan Tarrant
 * @since 9.1
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
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SecurityConfiguration that = (SecurityConfiguration) o;

      if (!authentication.equals(that.authentication)) return false;
      return ssl.equals(that.ssl);
   }

   @Override
   public int hashCode() {
      int result = authentication.hashCode();
      result = 31 * result + ssl.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "SecurityConfiguration{" +
            "authentication=" + authentication +
            ", ssl=" + ssl +
            '}';
   }
}
