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
   private final String keyStoreType;
   private final char[] keyStorePassword;
   private final String keyAlias;
   private final String protocol;
   private final String provider;
   private final SSLContext sslContext;
   private final String trustStoreFileName;
   private final String trustStoreType;
   private final char[] trustStorePassword;
   private final char[] keyStoreCertificatePassword;

   SslEngineConfiguration(String keyStoreFileName, String keyStoreType, char[] keyStorePassword, char[] keyStoreCertificatePassword, String keyAlias,
                          SSLContext sslContext, String trustStoreFileName, String trustStoreType, char[] trustStorePassword, String protocol, String provider) {
      this.keyStoreFileName = keyStoreFileName;
      this.keyStoreType = keyStoreType;
      this.keyStorePassword = keyStorePassword;
      this.keyStoreCertificatePassword = keyStoreCertificatePassword;
      this.keyAlias = keyAlias;
      this.sslContext = sslContext;
      this.trustStoreFileName = trustStoreFileName;
      this.trustStoreType = trustStoreType;
      this.trustStorePassword = trustStorePassword;
      this.protocol = protocol;
      this.provider = provider;
   }

   public String keyStoreFileName() {
      return keyStoreFileName;
   }

   public String keyStoreType() {
      return keyStoreType;
   }

   public char[] keyStorePassword() {
      return keyStorePassword;
   }

   public char[] keyStoreCertificatePassword() {
      return keyStoreCertificatePassword;
   }

   public String keyAlias() {
      return keyAlias;
   }

   public SSLContext sslContext() {
      return sslContext;
   }

   public String trustStoreFileName() {
      return trustStoreFileName;
   }

   public String trustStoreType() {
      return trustStoreType;
   }

   public char[] trustStorePassword() {
      return trustStorePassword;
   }

   public String protocol() {
      return protocol;
   }

   public String provider() {
      return provider;
   }

   public String[] protocols() {
      if (protocol != null) {
         return new String[]{protocol};
      } else {
         return null;
      }
   }

   @Override
   public String toString() {
      return "SslEngineConfiguration{" +
            "keyStoreFileName='" + keyStoreFileName + '\'' +
            ", keyStoreType='" + keyStoreType + '\'' +
            ", keyAlias='" + keyAlias + '\'' +
            ", protocol='" + protocol + '\'' +
            ", provider='" + provider + '\'' +
            ", sslContext=" + sslContext +
            ", trustStoreFileName='" + trustStoreFileName + '\'' +
            ", trustStoreType='" + trustStoreType + '\'' +
            '}';
   }
}
