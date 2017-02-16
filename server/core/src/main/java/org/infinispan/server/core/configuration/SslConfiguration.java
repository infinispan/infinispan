package org.infinispan.server.core.configuration;

import java.util.Map;

import javax.net.ssl.SSLContext;

/**
 * SslConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class SslConfiguration {

   public static final String DEFAULT_SNI_DOMAIN = "*";

   private final boolean enabled;
   private final boolean requireClientAuth;
   private final Map<String, SslEngineConfiguration> sniDomainsConfiguration;

   SslConfiguration(boolean enabled, boolean requireClientAuth, Map<String, SslEngineConfiguration> sniDomainsConfiguration) {
      this.enabled = enabled;
      this.requireClientAuth = requireClientAuth;
      this.sniDomainsConfiguration = sniDomainsConfiguration;
   }

   public boolean enabled() {
      return enabled;
   }

   public boolean requireClientAuth() {
      return requireClientAuth;
   }

   public String keyStoreFileName() {
      return sniDomainsConfiguration.get(DEFAULT_SNI_DOMAIN).keyStoreFileName();
   }

   public char[] keyStorePassword() {
      return sniDomainsConfiguration.get(DEFAULT_SNI_DOMAIN).keyStorePassword();
   }

   public char[] keyStoreCertificatePassword() {
      return sniDomainsConfiguration.get(DEFAULT_SNI_DOMAIN).keyStoreCertificatePassword();
   }

   public SSLContext sslContext() {
      return sniDomainsConfiguration.get(DEFAULT_SNI_DOMAIN).sslContext();
   }

   public String trustStoreFileName() {
      return sniDomainsConfiguration.get(DEFAULT_SNI_DOMAIN).trustStoreFileName();
   }

   public char[] trustStorePassword() {
      return sniDomainsConfiguration.get(DEFAULT_SNI_DOMAIN).trustStorePassword();
   }

   public Map<String, SslEngineConfiguration> sniDomainsConfiguration() {
      return sniDomainsConfiguration;
   }

   @Override
   public String toString() {
      return "SslConfiguration [" +
              "enabled=" + enabled +
              ", requireClientAuth=" + requireClientAuth +
              ", sniDomainsConfiguration=" + sniDomainsConfiguration +
              ']';
   }
}
