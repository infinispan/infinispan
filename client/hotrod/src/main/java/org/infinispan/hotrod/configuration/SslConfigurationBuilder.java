package org.infinispan.hotrod.configuration;

import static org.infinispan.hotrod.configuration.SslConfiguration.CIPHERS;
import static org.infinispan.hotrod.configuration.SslConfiguration.ENABLED;
import static org.infinispan.hotrod.configuration.SslConfiguration.KEYSTORE_FILENAME;
import static org.infinispan.hotrod.configuration.SslConfiguration.KEYSTORE_PASSWORD;
import static org.infinispan.hotrod.configuration.SslConfiguration.KEYSTORE_TYPE;
import static org.infinispan.hotrod.configuration.SslConfiguration.KEY_ALIAS;
import static org.infinispan.hotrod.configuration.SslConfiguration.PROTOCOL;
import static org.infinispan.hotrod.configuration.SslConfiguration.PROVIDER;
import static org.infinispan.hotrod.configuration.SslConfiguration.SNI_HOSTNAME;
import static org.infinispan.hotrod.configuration.SslConfiguration.SSL_CONTEXT;
import static org.infinispan.hotrod.configuration.SslConfiguration.TRUSTSTORE_FILENAME;
import static org.infinispan.hotrod.configuration.SslConfiguration.TRUSTSTORE_PASSWORD;
import static org.infinispan.hotrod.configuration.SslConfiguration.TRUSTSTORE_TYPE;
import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.util.List;
import java.util.Properties;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;

/**
 *
 * SSLConfigurationBuilder.
 *
 * @since 14.0
 */
public class SslConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<SslConfiguration> {
   private final AttributeSet attributes = SslConfiguration.attributeDefinitionSet();

   SslConfigurationBuilder(HotRodConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Disables the SSL support
    */
   public SslConfigurationBuilder disable() {
      return enabled(false);
   }

   /**
    * Enables the SSL support
    */
   public SslConfigurationBuilder enable() {
      return enabled(true);
   }

   /**
    * Enables or disables the SSL support
    */
   public SslConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   /**
    * Specifies the filename of a keystore to use to create the {@link SSLContext} You also need to
    * specify a {@link #keyStorePassword(char[])}. Alternatively specify an initialized {@link #sslContext(SSLContext)}.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder keyStoreFileName(String keyStoreFileName) {
      attributes.attribute(KEYSTORE_FILENAME).set(keyStoreFileName);
      return enable();
   }

   /**
    * Specifies the type of the keystore, such as JKS or JCEKS. Defaults to JKS.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder keyStoreType(String keyStoreType) {
      attributes.attribute(KEYSTORE_TYPE).set(keyStoreType);
      return enable();
   }

   /**
    * Specifies the password needed to open the keystore You also need to specify a
    * {@link #keyStoreFileName(String)}. Alternatively specify an initialized {@link #sslContext(SSLContext)}.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder keyStorePassword(char[] keyStorePassword) {
      attributes.attribute(KEYSTORE_PASSWORD).set(keyStorePassword);
      return enable();
   }

   /**
    * Sets the alias of the key to use, in case the keyStore contains multiple certificates.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder keyAlias(String keyAlias) {
      attributes.attribute(KEY_ALIAS).set(keyAlias);
      return enable();
   }

   /**
    * Specifies a pre-built {@link SSLContext}
    */
   public SslConfigurationBuilder sslContext(SSLContext sslContext) {
      attributes.attribute(SSL_CONTEXT).set(sslContext);
      return enable();
   }

   /**
    * Specifies the filename of a truststore to use to create the {@link SSLContext} You also need
    * to specify a {@link #trustStorePassword(char[])}. Alternatively specify an initialized {@link #sslContext(SSLContext)}.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder trustStoreFileName(String trustStoreFileName) {
      attributes.attribute(TRUSTSTORE_FILENAME).set(trustStoreFileName);
      return enable();
   }

   /**
    * Specifies the type of the truststore, such as JKS or JCEKS. Defaults to JKS.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder trustStoreType(String trustStoreType) {
      attributes.attribute(TRUSTSTORE_TYPE).set(trustStoreType);
      return enable();
   }

   /**
    * Specifies the password needed to open the truststore You also need to specify a
    * {@link #trustStoreFileName(String)}. Alternatively specify an initialized {@link #sslContext(SSLContext)}.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder trustStorePassword(char[] trustStorePassword) {
      attributes.attribute(TRUSTSTORE_PASSWORD).set(trustStorePassword);
      return enable();
   }

   /**
    * Specifies the TLS SNI hostname for the connection
    * @see javax.net.ssl.SSLParameters#setServerNames(List).
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder sniHostName(String sniHostName) {
      attributes.attribute(SNI_HOSTNAME).set(sniHostName);
      return enable();
   }

   /**
    * Configures the SSL provider.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    *
    * @see SSLContext#getInstance(String)
    * @param provider The name of the provider to use when obtaining an SSLContext.
    */
   public SslConfigurationBuilder provider(String provider) {
      attributes.attribute(PROVIDER).set(provider);
      return enable();
   }

   /**
    * Configures the secure socket protocol.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    *
    * @see SSLContext#getInstance(String)
    * @param protocol The standard name of the requested protocol, e.g TLSv1.2
    */
   public SslConfigurationBuilder protocol(String protocol) {
      attributes.attribute(PROTOCOL).set(protocol);
      return enable();
   }

   /**
    * Configures the ciphers
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    *
    * @see SSLContext#getInstance(String)
    * @param ciphers one or more cipher names
    */
   public SslConfigurationBuilder ciphers(String... ciphers) {
      attributes.attribute(CIPHERS).set(ciphers);
      return enable();
   }

   @Override
   public void validate() {
      if (attributes.attribute(ENABLED).get()) {
         if (attributes.attribute(SSL_CONTEXT).isNull()) {
            if (!attributes.attribute(KEYSTORE_FILENAME).isNull() && attributes.attribute(KEYSTORE_PASSWORD).isNull()) {
               throw HOTROD.missingKeyStorePassword(attributes.attribute(KEYSTORE_FILENAME).get());
            }
            if (attributes.attribute(TRUSTSTORE_FILENAME).isNull()) {
               throw HOTROD.noSSLTrustManagerConfiguration();
            }
            if (!attributes.attribute(TRUSTSTORE_FILENAME).isNull() && attributes.attribute(TRUSTSTORE_PASSWORD).isNull() && !"pem".equalsIgnoreCase(attributes.attribute(KEYSTORE_TYPE).get())) {
               throw HOTROD.missingTrustStorePassword(attributes.attribute(TRUSTSTORE_FILENAME).get());
            }
         } else {
            if (!attributes.attribute(KEYSTORE_FILENAME).isNull() || !attributes.attribute(TRUSTSTORE_FILENAME).isNull()) {
               throw HOTROD.xorSSLContext();
            }
         }
      }
   }

   @Override
   public SslConfiguration create() {
      return new SslConfiguration(attributes.protect());
   }

   @Override
   public Builder read(SslConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   @Override
   public HotRodConfigurationBuilder withProperties(Properties properties) {
      attributes.fromProperties(TypedProperties.toTypedProperties(properties), "org.infinispan.client.");
      return builder;
   }
}
