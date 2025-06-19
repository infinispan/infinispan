package org.infinispan.client.openapi.configuration;

import java.util.Arrays;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

/**
 * SslConfiguration.
 *
 * @author Tristan Tarrant
 * @since 16.0
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

   private final String provider;
   private final TrustManager[] trustManagers;
   private final HostnameVerifier hostnameVerifier;

   SslConfiguration(boolean enabled, String keyStoreFileName, String keyStoreType, char[] keyStorePassword, char[] keyStoreCertificatePassword, String keyAlias,
                    SSLContext sslContext, TrustManager[] trustManagers, HostnameVerifier hostnameVerifier,
                    String trustStoreFileName, String trustStorePath, String trustStoreType, char[] trustStorePassword, String sniHostName, String protocol,
                    String provider) {
      this.enabled = enabled;
      this.keyStoreFileName = keyStoreFileName;
      this.keyStoreType = keyStoreType;
      this.keyStorePassword = keyStorePassword;
      this.keyStoreCertificatePassword = keyStoreCertificatePassword;
      this.keyAlias = keyAlias;
      this.sslContext = sslContext;
      this.hostnameVerifier = hostnameVerifier;
      this.trustManagers = trustManagers;
      this.trustStoreFileName = trustStoreFileName;
      this.trustStorePath = trustStorePath;
      this.trustStoreType = trustStoreType;
      this.trustStorePassword = trustStorePassword;
      this.sniHostName = sniHostName;
      this.protocol = protocol;
      this.provider = provider;
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

   public TrustManager[] trustManagers() {
      return trustManagers;
   }

   public HostnameVerifier hostnameVerifier() {
      return hostnameVerifier;
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

   public String provider() {
      return provider;
   }

   @Override
   public String toString() {
      return "SslConfiguration{" +
            "enabled=" + enabled +
            ", keyStoreFileName='" + keyStoreFileName + '\'' +
            ", keyStoreType='" + keyStoreType + '\'' +
            ", keyAlias='" + keyAlias + '\'' +
            ", sslContext=" + sslContext +
            ", trustManagers=" + Arrays.toString(trustManagers) +
            ", trustStoreFileName='" + trustStoreFileName + '\'' +
            ", trustStoreType='" + trustStoreType + '\'' +
            ", sniHostName='" + sniHostName + '\'' +
            ", protocol='" + protocol + '\'' +
            ", provider='" + provider + '\'' +
            '}';
   }
}
