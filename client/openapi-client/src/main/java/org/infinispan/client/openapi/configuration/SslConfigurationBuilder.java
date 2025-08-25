package org.infinispan.client.openapi.configuration;

import java.util.List;
import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;

/**
 * SSLConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 16.0
 */
public class SslConfigurationBuilder extends AbstractSecurityConfigurationChildBuilder implements Builder<SslConfiguration> {
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
   private String provider;
   private TrustManager[] trustManagers;
   private HostnameVerifier hostnameVerifier;

   protected SslConfigurationBuilder(SecurityConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
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
    * Specifies the filename of a keystore to use to create the {@link SSLContext} You also need to specify a {@link
    * #keyStorePassword(char[])}. Alternatively specify an initialized {@link #sslContext(SSLContext)}. Setting this
    * property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder keyStoreFileName(String keyStoreFileName) {
      this.keyStoreFileName = keyStoreFileName;
      return enable();
   }

   /**
    * Specifies the type of the keystore, such as JKS or JCEKS. Defaults to JKS. Setting this property also implicitly
    * enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder keyStoreType(String keyStoreType) {
      this.keyStoreType = keyStoreType;
      return enable();
   }

   /**
    * Specifies the password needed to open the keystore You also need to specify a {@link #keyStoreFileName(String)}.
    * Alternatively specify an initialized {@link #sslContext(SSLContext)}. Setting this property also implicitly
    * enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder keyStorePassword(char[] keyStorePassword) {
      this.keyStorePassword = keyStorePassword;
      return enable();
   }

   /**
    * Sets the alias of the key to use, in case the keyStore contains multiple certificates. Setting this property also
    * implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder keyAlias(String keyAlias) {
      this.keyAlias = keyAlias;
      return enable();
   }

   /**
    * Sets an {@link SSLContext}
    */
   public SslConfigurationBuilder sslContext(SSLContext sslContext) {
      this.sslContext = sslContext;
      return enable();
   }

   /**
    * Sets the {@link TrustManager}s used to create the {@link SSLContext}t
    */
   public SslConfigurationBuilder trustManagers(TrustManager[] trustManagers) {
      this.trustManagers = trustManagers;
      return enable();
   }

   /**
    * Sets the {@link HostnameVerifier} to use when validating certificates against hostnames
    */
   public SslConfigurationBuilder hostnameVerifier(HostnameVerifier hostnameVerifier) {
      this.hostnameVerifier = hostnameVerifier;
      return enable();
   }

   /**
    * Specifies the filename of a truststore to use to create the {@link SSLContext} You also need to specify a {@link
    * #trustStorePassword(char[])}. Alternatively specify an initialized {@link #sslContext(SSLContext)}. Setting this
    * property also implicitly enables SSL/TLS (see {@link #enable()}
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
    * Specifies the type of the truststore, such as JKS or JCEKS. Defaults to JKS. Setting this property also implicitly
    * enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder trustStoreType(String trustStoreType) {
      this.trustStoreType = trustStoreType;
      return enable();
   }

   /**
    * Specifies the password needed to open the truststore You also need to specify a {@link
    * #trustStoreFileName(String)}. Alternatively specify an initialized {@link #sslContext(SSLContext)}. Setting this
    * property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder trustStorePassword(char[] trustStorePassword) {
      this.trustStorePassword = trustStorePassword;
      return enable();
   }

   /**
    * Specifies the TLS SNI hostname for the connection.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    *
    * @see javax.net.ssl.SSLParameters#setServerNames(List)
    */
   public SslConfigurationBuilder sniHostName(String sniHostName) {
      this.sniHostName = sniHostName;
      return enable();
   }

   /**
    * Configures the secure socket protocol. Setting this property also implicitly enables SSL/TLS (see {@link
    * #enable()}
    *
    * @param protocol The standard name of the requested protocol, e.g TLSv1.2
    * @see SSLContext#getInstance(String)
    */
   public SslConfigurationBuilder protocol(String protocol) {
      this.protocol = protocol;
      return enable();
   }

   /**
    * Configures the security provider to use when initializing TLS. Setting this property also implicitly enables SSL/TLS (see {@link
    * #enable()}
    *
    * @param provider The name of a security provider
    * @see SSLContext#getInstance(String)
    */
   public SslConfigurationBuilder provider(String provider) {
      this.provider = provider;
      return enable();
   }

   @Override
   public void validate() {
      if (enabled) {
         if (sslContext == null) {
            if (keyStoreFileName != null && keyStorePassword == null) {
               throw new IllegalStateException("Missing key store password");
            }
            if (trustStoreFileName == null && trustStorePath == null) {
               throw new IllegalStateException("No SSL TrustStore configuration");
            }
            if (trustStoreFileName != null && trustStorePath != null) {
               throw new IllegalStateException("trustStoreFileName and trustStorePath are mutually exclusive");
            }
            if (trustStoreFileName != null && trustStorePassword == null) {
               throw new IllegalStateException("Missing trust store password ");
            }
         } else {
            if (keyStoreFileName != null || trustStoreFileName != null) {
               throw new IllegalStateException("SSLContext and stores are mutually exclusive");
            }
            if (trustManagers == null) {
               throw new IllegalStateException("SSLContext requires configuration of the TrustManagers");
            }
         }
      }
   }

   @Override
   public SslConfiguration create() {
      return new SslConfiguration(enabled,
            keyStoreFileName, keyStoreType, keyStorePassword, keyStoreCertificatePassword, keyAlias,
            sslContext, trustManagers, hostnameVerifier,
            trustStoreFileName, trustStorePath, trustStoreType, trustStorePassword,
            sniHostName, protocol, provider);
   }

   @Override
   public SslConfigurationBuilder read(SslConfiguration template, Combine combine) {
      this.enabled = template.enabled();
      this.keyStoreFileName = template.keyStoreFileName();
      this.keyStoreType = template.keyStoreType();
      this.keyStorePassword = template.keyStorePassword();
      this.keyStoreCertificatePassword = template.keyStoreCertificatePassword();
      this.keyAlias = template.keyAlias();
      this.sslContext = template.sslContext();
      this.hostnameVerifier = template.hostnameVerifier();
      this.trustManagers = template.trustManagers();
      this.trustStoreFileName = template.trustStoreFileName();
      this.trustStorePath = template.trustStorePath();
      this.trustStoreType = template.trustStoreType();
      this.trustStorePassword = template.trustStorePassword();
      this.sniHostName = template.sniHostName();
      this.protocol = template.protocol();
      this.provider = template.provider();
      return this;
   }

   @Override
   public OpenAPIClientConfigurationBuilder withProperties(Properties properties) {
      TypedProperties typed = TypedProperties.toTypedProperties(properties);

      if (typed.containsKey(OpenAPIClientConfigurationProperties.KEY_STORE_FILE_NAME))
         this.keyStoreFileName(typed.getProperty(OpenAPIClientConfigurationProperties.KEY_STORE_FILE_NAME, keyStoreFileName, true));

      if (typed.containsKey(OpenAPIClientConfigurationProperties.KEY_STORE_TYPE))
         this.keyStoreType(typed.getProperty(OpenAPIClientConfigurationProperties.KEY_STORE_TYPE, null, true));

      if (typed.containsKey(OpenAPIClientConfigurationProperties.KEY_STORE_PASSWORD))
         this.keyStorePassword(typed.getProperty(OpenAPIClientConfigurationProperties.KEY_STORE_PASSWORD, null, true).toCharArray());

      if (typed.containsKey(OpenAPIClientConfigurationProperties.KEY_ALIAS))
         this.keyAlias(typed.getProperty(OpenAPIClientConfigurationProperties.KEY_ALIAS, null, true));

      if (typed.containsKey(OpenAPIClientConfigurationProperties.TRUST_STORE_FILE_NAME))
         this.trustStoreFileName(typed.getProperty(OpenAPIClientConfigurationProperties.TRUST_STORE_FILE_NAME, trustStoreFileName, true));

      if (typed.containsKey(OpenAPIClientConfigurationProperties.TRUST_STORE_PATH))
         this.trustStorePath(typed.getProperty(OpenAPIClientConfigurationProperties.TRUST_STORE_PATH, trustStorePath, true));

      if (typed.containsKey(OpenAPIClientConfigurationProperties.TRUST_STORE_TYPE))
         this.trustStoreType(typed.getProperty(OpenAPIClientConfigurationProperties.TRUST_STORE_TYPE, null, true));

      if (typed.containsKey(OpenAPIClientConfigurationProperties.TRUST_STORE_PASSWORD))
         this.trustStorePassword(typed.getProperty(OpenAPIClientConfigurationProperties.TRUST_STORE_PASSWORD, null, true).toCharArray());

      if (typed.containsKey(OpenAPIClientConfigurationProperties.SSL_PROTOCOL))
         this.protocol(typed.getProperty(OpenAPIClientConfigurationProperties.SSL_PROTOCOL, null, true));

      if (typed.containsKey(OpenAPIClientConfigurationProperties.PROVIDER))
         this.provider(typed.getProperty(OpenAPIClientConfigurationProperties.PROVIDER, null, true));

      if (typed.containsKey(OpenAPIClientConfigurationProperties.SNI_HOST_NAME))
         this.sniHostName(typed.getProperty(OpenAPIClientConfigurationProperties.SNI_HOST_NAME, null, true));

      if (typed.containsKey(OpenAPIClientConfigurationProperties.SSL_CONTEXT))
         this.sslContext((SSLContext) typed.get(OpenAPIClientConfigurationProperties.SSL_CONTEXT));

      if (typed.containsKey(OpenAPIClientConfigurationProperties.USE_SSL))
         this.enabled(typed.getBooleanProperty(OpenAPIClientConfigurationProperties.USE_SSL, enabled, true));

      return builder.getBuilder();
   }
}
