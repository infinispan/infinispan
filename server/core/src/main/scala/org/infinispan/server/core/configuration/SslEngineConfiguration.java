package org.infinispan.server.core.configuration;

import javax.net.ssl.SSLContext;

/**
 * SslEngineConfiguration
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
public class SslEngineConfiguration {

   private final String keyStoreFileName;
   private final char[] keyStorePassword;
   private final SSLContext sslContext;
   private final String trustStoreFileName;
   private final char[] trustStorePassword;
   private final char[] keyStoreCertificatePassword;

   SslEngineConfiguration(String keyStoreFileName, char[] keyStorePassword, char[] keyStoreCertificatePassword, SSLContext sslContext, String trustStoreFileName, char[] trustStorePassword) {
      this.keyStoreFileName = keyStoreFileName;
      this.keyStorePassword = keyStorePassword;
      this.keyStoreCertificatePassword = keyStoreCertificatePassword;
      this.sslContext = sslContext;
      this.trustStoreFileName = trustStoreFileName;
      this.trustStorePassword = trustStorePassword;
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
      return "SslEngineConfiguration [" +
              "keyStoreFileName='" + keyStoreFileName + '\'' +
              ", keyStorePassword=***" +
              ", sslContext=" + sslContext +
              ", trustStoreFileName='" + trustStoreFileName + '\'' +
              ", trustStorePassword=***" +
              ", keyStoreCertificatePassword=***" +
              ']';
   }
}
