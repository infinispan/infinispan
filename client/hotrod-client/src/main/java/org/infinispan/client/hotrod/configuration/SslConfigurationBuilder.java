package org.infinispan.client.hotrod.configuration;

import java.util.List;
import java.util.Properties;

import javax.net.ssl.SSLContext;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.impl.TypedProperties;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.Builder;

/**
 *
 * SSLConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class SslConfigurationBuilder extends AbstractSecurityConfigurationChildBuilder implements Builder<SslConfiguration> {
   private static final Log log = LogFactory.getLog(SslConfigurationBuilder.class);
   private boolean enabled = false;
   private String keyStoreFileName;
   private String keyStoreType;
   private char[] keyStorePassword;
   private char[] keyStoreCertificatePassword;
   private String keyAlias;
   private String trustStorePath;
   private String trustStoreFileName;
   private String trustStoreType;
   private char[] trustStorePassword;
   private SSLContext sslContext;
   private String sniHostName;
   private String protocol;

   protected SslConfigurationBuilder(SecurityConfigurationBuilder builder) {
      super(builder);
   }

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
    * Specifies the filename of a keystore to use to create the {@link SSLContext} You also need to
    * specify a {@link #keyStorePassword(char[])}. Alternatively specify an initialized {@link #sslContext(SSLContext)}.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder keyStoreFileName(String keyStoreFileName) {
      this.keyStoreFileName = keyStoreFileName;
      return enable();
   }

   /**
    * Specifies the type of the keystore, such as JKS or JCEKS. Defaults to JKS.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder keyStoreType(String keyStoreType) {
      this.keyStoreType = keyStoreType;
      return enable();
   }

   /**
    * Specifies the password needed to open the keystore You also need to specify a
    * {@link #keyStoreFileName(String)}. Alternatively specify an initialized {@link #sslContext(SSLContext)}.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder keyStorePassword(char[] keyStorePassword) {
      this.keyStorePassword = keyStorePassword;
      return enable();
   }

   /**
    * Specifies the password needed to access private key associated with certificate stored in specified
    * {@link #keyStoreFileName(String)}. If password is not specified, password provided in
    * {@link #keyStorePassword(char[])} will be used.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder keyStoreCertificatePassword(char[] keyStoreCertificatePassword) {
      this.keyStoreCertificatePassword = keyStoreCertificatePassword;
      return enable();
   }

   /**
    * Sets the alias of the key to use, in case the keyStore contains multiple certificates.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder keyAlias(String keyAlias) {
      this.keyAlias = keyAlias;
      return enable();
   }

   public SslConfigurationBuilder sslContext(SSLContext sslContext) {
      this.sslContext = sslContext;
      return enable();
   }

   /**
    * Specifies the filename of a truststore to use to create the {@link SSLContext} You also need
    * to specify a {@link #trustStorePassword(char[])}. Alternatively specify an initialized {@link #sslContext(SSLContext)}.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder trustStoreFileName(String trustStoreFileName) {
      this.trustStoreFileName = trustStoreFileName;
      return enable();
   }

   /**
    * Specifies a path containing certificates in PEM format. An in-memory {@link java.security.KeyStore} will be built
    * with all the certificates found undert that path. This is mutually exclusive with {@link #trustStoreFileName}
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder trustStorePath(String trustStorePath) {
      this.trustStorePath = trustStorePath;
      return enable();
   }

   /**
    * Specifies the type of the truststore, such as JKS or JCEKS. Defaults to JKS.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder trustStoreType(String trustStoreType) {
      this.trustStoreType = trustStoreType;
      return enable();
   }

   /**
    * Specifies the password needed to open the truststore You also need to specify a
    * {@link #trustStoreFileName(String)}. Alternatively specify an initialized {@link #sslContext(SSLContext)}.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder trustStorePassword(char[] trustStorePassword) {
      this.trustStorePassword = trustStorePassword;
      return enable();
   }

   /**
    * Specifies the TLS SNI hostname for the connection
    * @see javax.net.ssl.SSLParameters#setServerNames(List).
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
     */
   public SslConfigurationBuilder sniHostName(String sniHostName) {
      this.sniHostName = sniHostName;
      return enable();
   }

   /**
    * Configures the secure socket protocol.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    *
    * @see javax.net.ssl.SSLContext#getInstance(String)
    * @param protocol The standard name of the requested protocol, e.g TLSv1.2
    */
   public SslConfigurationBuilder protocol(String protocol) {
      this.protocol = protocol;
      return enable();
   }

   @Override
   public void validate() {
      if (enabled) {
         if (sslContext == null) {
            if (keyStoreFileName != null && keyStorePassword == null) {
               throw log.missingKeyStorePassword(keyStoreFileName);
            }
            if (trustStoreFileName == null && trustStorePath == null) {
               throw log.noSSLTrustManagerConfiguration();
            }
            if (trustStoreFileName != null && trustStorePath != null) {
               throw log.trustStoreFileAndPathExclusive();
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
      return new SslConfiguration(enabled,
            keyStoreFileName, keyStoreType, keyStorePassword, keyStoreCertificatePassword, keyAlias,
            sslContext,
            trustStoreFileName, trustStorePath, trustStoreType, trustStorePassword,
            sniHostName, protocol);
   }

   @Override
   public SslConfigurationBuilder read(SslConfiguration template) {
      this.enabled = template.enabled();
      this.keyStoreFileName = template.keyStoreFileName();
      this.keyStoreType = template.keyStoreType();
      this.keyStorePassword = template.keyStorePassword();
      this.keyStoreCertificatePassword = template.keyStoreCertificatePassword();
      this.keyAlias = template.keyAlias();
      this.sslContext = template.sslContext();
      this.trustStoreFileName = template.trustStoreFileName();
      this.trustStorePath = template.trustStorePath();
      this.trustStoreType = template.trustStoreType();
      this.trustStorePassword = template.trustStorePassword();
      this.sniHostName = template.sniHostName();
      this.protocol = template.protocol();
      return this;
   }

   @Override
   public ConfigurationBuilder withProperties(Properties properties) {
      TypedProperties typed = TypedProperties.toTypedProperties(properties);

      if (typed.containsKey(ConfigurationProperties.KEY_STORE_FILE_NAME))
         this.keyStoreFileName(typed.getProperty(ConfigurationProperties.KEY_STORE_FILE_NAME, keyStoreFileName, true));

      if (typed.containsKey(ConfigurationProperties.KEY_STORE_TYPE))
         this.keyStoreType(typed.getProperty(ConfigurationProperties.KEY_STORE_TYPE, null, true));

      if (typed.containsKey(ConfigurationProperties.KEY_STORE_PASSWORD))
         this.keyStorePassword(typed.getProperty(ConfigurationProperties.KEY_STORE_PASSWORD, null, true).toCharArray());

      if (typed.containsKey(ConfigurationProperties.KEY_STORE_CERTIFICATE_PASSWORD))
         this.keyStoreCertificatePassword(typed.getProperty(ConfigurationProperties.KEY_STORE_CERTIFICATE_PASSWORD, null, true).toCharArray());

      if (typed.containsKey(ConfigurationProperties.KEY_ALIAS))
         this.keyAlias(typed.getProperty(ConfigurationProperties.KEY_ALIAS, null, true));

      if (typed.containsKey(ConfigurationProperties.TRUST_STORE_FILE_NAME))
         this.trustStoreFileName(typed.getProperty(ConfigurationProperties.TRUST_STORE_FILE_NAME, trustStoreFileName, true));

      if (typed.containsKey(ConfigurationProperties.TRUST_STORE_PATH))
         this.trustStorePath(typed.getProperty(ConfigurationProperties.TRUST_STORE_PATH, trustStorePath, true));

      if (typed.containsKey(ConfigurationProperties.TRUST_STORE_TYPE))
         this.trustStoreType(typed.getProperty(ConfigurationProperties.TRUST_STORE_TYPE, null, true));

      if (typed.containsKey(ConfigurationProperties.TRUST_STORE_PASSWORD))
         this.trustStorePassword(typed.getProperty(ConfigurationProperties.TRUST_STORE_PASSWORD, null, true).toCharArray());

      if(typed.containsKey(ConfigurationProperties.SSL_PROTOCOL))
         this.protocol(typed.getProperty(ConfigurationProperties.SSL_PROTOCOL, null, true));

      if (typed.containsKey(ConfigurationProperties.SNI_HOST_NAME))
         this.sniHostName(typed.getProperty(ConfigurationProperties.SNI_HOST_NAME, null, true));

      if (typed.containsKey(ConfigurationProperties.SSL_CONTEXT))
         this.sslContext((SSLContext) typed.get(ConfigurationProperties.SSL_CONTEXT));

      if (typed.containsKey(ConfigurationProperties.USE_SSL))
         this.enabled(typed.getBooleanProperty(ConfigurationProperties.USE_SSL, enabled, true));

      return builder.getBuilder();
   }
}
