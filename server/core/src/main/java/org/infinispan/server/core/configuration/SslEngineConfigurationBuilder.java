package org.infinispan.server.core.configuration;

import org.infinispan.server.core.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

/**
 *
 * SSLConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class SslEngineConfigurationBuilder implements SslConfigurationChildBuilder {
   private static final Log log = LogFactory.getLog(SslEngineConfigurationBuilder.class, Log.class);
   private final SslConfigurationBuilder parentSslConfigurationBuilder;
   private String keyStoreFileName;
   private char[] keyStorePassword;
   private SSLContext sslContext;
   private String trustStoreFileName;
   private char[] trustStorePassword;
   private char[] keyStoreCertificatePassword;
   private String domain = "*";

   SslEngineConfigurationBuilder(SslConfigurationBuilder parentSslConfigurationBuilder) {
      this.parentSslConfigurationBuilder = parentSslConfigurationBuilder;
   }

   /**
    * Sets the {@link SSLContext} to use for setting up SSL connections.
    */
   public SslEngineConfigurationBuilder sslContext(SSLContext sslContext) {
      this.sslContext = sslContext;
      return this;
   }

   /**
    * Specifies the filename of a keystore to use to create the {@link SSLContext} You also need to
    * specify a {@link #keyStorePassword(String)}. Alternatively specify an array of
    * {@link #keyManagers(KeyManager[])}
    */
   public SslEngineConfigurationBuilder keyStoreFileName(String keyStoreFileName) {
      this.keyStoreFileName = keyStoreFileName;
      return this;
   }

   /**
    * Specifies the password needed to open the keystore You also need to specify a
    * {@link #keyStoreFileName(String)} Alternatively specify an array of
    * {@link #keyManagers(KeyManager[])}
    */
   public SslEngineConfigurationBuilder keyStorePassword(char[] keyStorePassword) {
      this.keyStorePassword = keyStorePassword;
      return this;
   }

   /**
    * Specifies the filename of a truststore to use to create the {@link SSLContext} You also need
    * to specify a {@link #trustStorePassword(String)}. Alternatively specify an array of
    * {@link #trustManagers(TrustManager[])}
    */
   public SslEngineConfigurationBuilder trustStoreFileName(String trustStoreFileName) {
      this.trustStoreFileName = trustStoreFileName;
      return this;
   }

   /**
    * Specifies the password needed to open the truststore You also need to specify a
    * {@link #trustStoreFileName(String)} Alternatively specify an array of
    * {@link #trustManagers(TrustManager[])}
    */
   public SslEngineConfigurationBuilder trustStorePassword(char[] trustStorePassword) {
      this.trustStorePassword = trustStorePassword;
      return this;
   }

   /**
    * Specifies the password needed to access private key associated with certificate stored in specified
    * {@link #keyStoreFileName(String)}. If password is not specified, password provided in
    * {@link #keyStorePassword(String)} will be used.
    */
   public SslEngineConfigurationBuilder keyStoreCertificatePassword(char[] keyStoreCertificatePassword) {
      this.keyStoreCertificatePassword = keyStoreCertificatePassword;
      return this;
   }

   @Override
   public void validate() {
      if(domain == null) {
         throw log.noSniDomainConfigured();
      }
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

   @Override
   public SslEngineConfiguration create() {
      return new SslEngineConfiguration(keyStoreFileName, keyStorePassword, keyStoreCertificatePassword, sslContext, trustStoreFileName, trustStorePassword);
   }

   @Override
   public SslEngineConfigurationBuilder read(SslEngineConfiguration template) {
      this.keyStoreFileName = template.keyStoreFileName();
      this.keyStorePassword = template.keyStorePassword();
      this.sslContext = template.sslContext();
      this.trustStoreFileName = template.trustStoreFileName();
      this.trustStorePassword = template.trustStorePassword();
      return this;
   }

   @Override
   public SslEngineConfigurationBuilder sniHostName(String domain) {
      return parentSslConfigurationBuilder.sniHostName(domain);
   }
}
