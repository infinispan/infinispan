package org.infinispan.client.hotrod.configuration;

import java.util.Collection;

import javax.net.ssl.SSLContext;

/**
 * SslConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class SslConfiguration {
   private final boolean enabled;
   private final String keyStoreFileName;
   private final String keyStoreType;
   private final char[] keyStorePassword;
   private final char[] keyStoreCertificatePassword;
   private final String keyAlias;
   private final SSLContext sslContext;
   private final String trustStoreFileName;
   private final String trustStorePath;
   private final String trustStoreType;
   private final char[] trustStorePassword;
   private final String sniHostName;
   private final String protocol;
   private final Collection<String> ciphers;
   private final String provider;
   private final boolean hostnameValidation;

   SslConfiguration(boolean enabled, String keyStoreFileName, String keyStoreType, char[] keyStorePassword, char[] keyStoreCertificatePassword, String keyAlias,
                    SSLContext sslContext,
                    String trustStoreFileName, String trustStorePath, String trustStoreType, char[] trustStorePassword, String sniHostName, String provider, String protocol, Collection<String> ciphers,
                    boolean hostnameValidation) {
      this.enabled = enabled;
      this.keyStoreFileName = keyStoreFileName;
      this.keyStoreType = keyStoreType;
      this.keyStorePassword = keyStorePassword;
      this.keyStoreCertificatePassword = keyStoreCertificatePassword;
      this.keyAlias = keyAlias;
      this.sslContext = sslContext;
      this.trustStoreFileName = trustStoreFileName;
      this.trustStorePath = trustStorePath;
      this.trustStoreType = trustStoreType;
      this.trustStorePassword = trustStorePassword;
      this.sniHostName = sniHostName;
      this.provider = provider;
      this.protocol = protocol;
      this.ciphers = ciphers;
      this.hostnameValidation = hostnameValidation;
   }

   public boolean enabled() {
      return enabled;
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

   public String trustStorePath() {
      return trustStorePath;
   }

   public String trustStoreType() {
      return trustStoreType;
   }

   public char[] trustStorePassword() {
      return trustStorePassword;
   }

   public String sniHostName() {
      return sniHostName;
   }

   public String protocol() {
      return protocol;
   }

   public Collection<String> ciphers() {
      return ciphers;
   }

   public String provider() {
      return provider;
   }

   public boolean hostnameValidation() {
      return hostnameValidation;
   }

   @Override
   public String toString() {
      return "SslConfiguration{" +
            "enabled=" + enabled +
            ", keyStoreFileName='" + keyStoreFileName + '\'' +
            ", keyStoreType='" + keyStoreType + '\'' +
            ", keyAlias='" + keyAlias + '\'' +
            ", sslContext=" + sslContext +
            ", trustStoreFileName='" + trustStoreFileName + '\'' +
            ", trustStoreType='" + trustStoreType + '\'' +
            ", sniHostName='" + sniHostName + '\'' +
            ", provider='" + provider +'\'' +
            ", protocol='" + protocol + '\'' +
            ", ciphers='" + ciphers  + '\'' +
            ", hostnameValidation=" + hostnameValidation +
            '}';
   }
}
