package org.infinispan.server.core.configuration;

import javax.net.ssl.SSLContext;

/**
 * SslConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class SslConfiguration {
   private final boolean enabled;
   private final boolean requireClientAuth;
   private final String keyStoreFileName;
   private final char[] keyStorePassword;
   private final char[] keyStoreCertificatePassword;
   private final SSLContext sslContext;
   private final String trustStoreFileName;
   private final char[] trustStorePassword;

   SslConfiguration(boolean enabled, boolean requireClientAuth, String keyStoreFileName, char[] keyStorePassword, SSLContext sslContext, String trustStoreFileName,
                    char[] trustStorePassword) {
      this(enabled, requireClientAuth, keyStoreFileName, keyStorePassword, null, sslContext, trustStoreFileName, trustStorePassword);
   }

   SslConfiguration(boolean enabled, boolean requireClientAuth, String keyStoreFileName, char[] keyStorePassword, char[] keyStoreCertificatePassword, SSLContext sslContext, String trustStoreFileName,
         char[] trustStorePassword) {
      this.enabled = enabled;
      this.requireClientAuth = requireClientAuth;
      this.keyStoreFileName = keyStoreFileName;
      this.keyStorePassword = keyStorePassword;
      this.keyStoreCertificatePassword = keyStoreCertificatePassword;
      this.sslContext = sslContext;
      this.trustStoreFileName = trustStoreFileName;
      this.trustStorePassword = trustStorePassword;
   }

   public boolean enabled() {
      return enabled;
   }

   public boolean requireClientAuth() {
      return requireClientAuth;
   }

   public String keyStoreFileName() {
      return keyStoreFileName;
   }

   public char[] keyStorePassword() {
      return keyStorePassword;
   }

   public char[] keyStoreCertificatePassword() {
      return keyStoreCertificatePassword;
   }

   public SSLContext sslContext() {
      return sslContext;
   }

   public String trustStoreFileName() {
      return trustStoreFileName;
   }

   public char[] trustStorePassword() {
      return trustStorePassword;
   }

   @Override
   public String toString() {
      return "SslConfiguration [enabled=" + enabled + ", requireClientAuth=" + requireClientAuth + ", keyStoreFileName=" + keyStoreFileName + ", sslContext=" + sslContext
            + ", trustStoreFileName=" + trustStoreFileName + "]";
   }
}
