package org.infinispan.server.core.configuration;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.server.core.logging.JavaLog;
import org.infinispan.util.logging.LogFactory;

/**
 *
 * SSLConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class SslConfigurationBuilder implements Builder<SslConfiguration> {
   private static final JavaLog log = LogFactory.getLog(SslConfigurationBuilder.class, JavaLog.class);
   private boolean enabled = false;
   private boolean requireClientAuth = false;
   private String keyStoreFileName;
   private char[] keyStorePassword;
   private char[] keyStoreCertificatePassword;
   private SSLContext sslContext;
   private String trustStoreFileName;
   private char[] trustStorePassword;

   SslConfigurationBuilder() {}

   /**
    * Disables the SSL support
    */
   public SslConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   /**
    * Enables the SSL support
    */
   public SslConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }

   /**
    * Enables or disables the SSL support
    */
   public SslConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   /**
    * Enables client certificate authentication
    */
   public SslConfigurationBuilder requireClientAuth(boolean requireClientAuth) {
      this.requireClientAuth = requireClientAuth;
      return this;
   }

   /**
    * Sets the {@link SSLContext} to use for setting up SSL connections.
    */
   public SslConfigurationBuilder sslContext(SSLContext sslContext) {
      this.sslContext = sslContext;
      return this;
   }

   /**
    * Specifies the filename of a keystore to use to create the {@link SSLContext} You also need to
    * specify a {@link #keyStorePassword(String)}. Alternatively specify an array of
    * {@link #keyManagers(KeyManager[])}
    */
   public SslConfigurationBuilder keyStoreFileName(String keyStoreFileName) {
      this.keyStoreFileName = keyStoreFileName;
      return this;
   }

   /**
    * Specifies the password needed to open the keystore You also need to specify a
    * {@link #keyStoreFileName(String)} Alternatively specify an array of
    * {@link #keyManagers(KeyManager[])}
    */
   public SslConfigurationBuilder keyStorePassword(char[] keyStorePassword) {
      this.keyStorePassword = keyStorePassword;
      return this;
   }

   /**
    * Specifies the password needed to access private key associated with certificate stored in specified
    * {@link #keyStoreFileName(String)}. If password is not specified, password provided in
    * {@link #keyStorePassword(String)} will be used.
    */
   public SslConfigurationBuilder keyStoreCertificatePassword(char[] keyStoreCertificatePassword) {
      this.keyStoreCertificatePassword = keyStoreCertificatePassword;
      return this;
   }

   /**
    * Specifies the filename of a truststore to use to create the {@link SSLContext} You also need
    * to specify a {@link #trustStorePassword(String)}. Alternatively specify an array of
    * {@link #trustManagers(TrustManager[])}
    */
   public SslConfigurationBuilder trustStoreFileName(String trustStoreFileName) {
      this.trustStoreFileName = trustStoreFileName;
      return this;
   }

   /**
    * Specifies the password needed to open the truststore You also need to specify a
    * {@link #trustStoreFileName(String)} Alternatively specify an array of
    * {@link #trustManagers(TrustManager[])}
    */
   public SslConfigurationBuilder trustStorePassword(char[] trustStorePassword) {
      this.trustStorePassword = trustStorePassword;
      return this;
   }

   @Override
   public void validate() {
      if (enabled) {
         if (sslContext == null) {
            if (keyStoreFileName == null) {
               throw log.noSSLKeyManagerConfiguration();
            }
            if (keyStoreFileName != null && keyStorePassword == null) {
               throw log.missingKeyStorePassword(keyStoreFileName);
            }
            if (trustStoreFileName == null) {
               throw log.noSSLTrustManagerConfiguration();
            }
            if (trustStoreFileName != null && trustStorePassword == null) {
               throw log.missingTrustStorePassword(trustStoreFileName);
            }
         } else {
            if (keyStoreFileName != null || trustStoreFileName != null) {
               throw log.xorSSLContext();
            }
         }
      }
   }

   @Override
   public SslConfiguration create() {
      return new SslConfiguration(enabled, requireClientAuth, keyStoreFileName, keyStorePassword, keyStoreCertificatePassword, sslContext, trustStoreFileName, trustStorePassword);
   }

   @Override
   public SslConfigurationBuilder read(SslConfiguration template) {
      this.enabled = template.enabled();
      this.requireClientAuth = template.requireClientAuth();
      this.keyStoreFileName = template.keyStoreFileName();
      this.keyStorePassword = template.keyStorePassword();
      this.keyStoreCertificatePassword = template.keyStoreCertificatePassword();
      this.sslContext = template.sslContext();
      this.trustStoreFileName = template.trustStoreFileName();
      this.trustStorePassword = template.trustStorePassword();
      return this;
   }

}
