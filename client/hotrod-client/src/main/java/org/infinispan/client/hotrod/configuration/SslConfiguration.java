package org.infinispan.client.hotrod.configuration;

import javax.net.ssl.SSLContext;
import java.util.Arrays;

/**
 * SslConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class SslConfiguration {
   private final boolean enabled;
   private final String keyStoreFileName;
   private final char[] keyStorePassword;
   private final char[] keyStoreCertificatePassword;
   private final SSLContext sslContext;
   private final String trustStoreFileName;
   private final char[] trustStorePassword;
   private String sniHostName;

   SslConfiguration(boolean enabled, String keyStoreFileName, char[] keyStorePassword, char[] keyStoreCertificatePassword, SSLContext sslContext, String trustStoreFileName,
                    char[] trustStorePassword, String sniHostName) {
      this.enabled = enabled;
      this.keyStoreFileName = keyStoreFileName;
      this.keyStorePassword = keyStorePassword;
      this.keyStoreCertificatePassword = keyStoreCertificatePassword;
      this.sslContext = sslContext;
      this.trustStoreFileName = trustStoreFileName;
      this.trustStorePassword = trustStorePassword;
      this.sniHostName = sniHostName;
   }

   public boolean enabled() {
      return enabled;
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
      return "SslConfiguration [" +
              "keyStoreFileName='" + keyStoreFileName + '\'' +
              ", enabled=" + enabled +
              ", keyStoreCertificatePassword=" + Arrays.toString(keyStoreCertificatePassword) +
              ", sslContext=" + sslContext +
              ", trustStoreFileName='" + trustStoreFileName + '\'' +
              ", sniHostName=" + sniHostName +
              ']';
   }

   public String sniHostName() {
      return sniHostName;
   }
}
