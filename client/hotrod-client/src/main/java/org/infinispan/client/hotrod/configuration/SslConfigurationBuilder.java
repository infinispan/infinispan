package org.infinispan.client.hotrod.configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import javax.net.ssl.SSLContext;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

/**
 *
 * SSLConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class SslConfigurationBuilder extends AbstractSecurityConfigurationChildBuilder implements Builder<SslConfiguration> {
   private boolean enabled = false;
   private String keyStoreFileName;
   private String keyStoreType;
   private char[] keyStorePassword;
   private String keyAlias;
   private String trustStorePath;
   private String trustStoreFileName;
   private String trustStoreType;
   private char[] trustStorePassword;
   private SSLContext sslContext;
   private String sniHostName;
   private String protocol;
   private Collection<String> ciphers;
   private String provider;
   private boolean hostnameValidation = true;

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
    * Sets the alias of the key to use, in case the keyStore contains multiple certificates.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    */
   public SslConfigurationBuilder keyAlias(String keyAlias) {
      this.keyAlias = keyAlias;
      return enable();
   }

   /**
    * Specifies a pre-built {@link SSLContext}
    */
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
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    *
    * @see javax.net.ssl.SSLParameters#setServerNames(List)
     */
   public SslConfigurationBuilder sniHostName(String sniHostName) {
      this.sniHostName = sniHostName;
      return enable();
   }

   /**
    * Configures the SSL provider.
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    *
    * @see javax.net.ssl.SSLContext#getInstance(String)
    * @param provider The name of the provider to use when obtaining an SSLContext.
    */
   public SslConfigurationBuilder provider(String provider) {
      this.provider = provider;
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

   /**
    * Configures the ciphers
    * Setting this property also implicitly enables SSL/TLS (see {@link #enable()}
    *
    * @see javax.net.ssl.SSLContext#getInstance(String)
    * @param ciphers one or more cipher names
    */
   public SslConfigurationBuilder ciphers(String... ciphers) {
      this.ciphers = Arrays.asList(ciphers);
      return enable();
   }

   /**
    * Configures whether to enable hostname validation according to <a href="https://datatracker.ietf.org/doc/html/rfc2818">RFC 2818</a>.
    * This is enabled by default and requires that the server certificate includes a subjectAltName extension of type dNSName or iPAddress.
    *
    * @param hostnameValidation whether to enable hostname validation
    */
   public SslConfigurationBuilder hostnameValidation(boolean hostnameValidation) {
      this.hostnameValidation = hostnameValidation;
      return enable();
   }

   @Override
   public void validate() {
      if (enabled) {
         if (sslContext == null) {
            if (keyStoreFileName != null && keyStorePassword == null) {
               throw HOTROD.missingKeyStorePassword(keyStoreFileName);
            }
            if (trustStoreFileName == null && trustStorePath == null) {
               throw HOTROD.noSSLTrustManagerConfiguration();
            }
            if (trustStoreFileName != null && trustStorePath != null) {
               throw HOTROD.trustStoreFileAndPathExclusive();
            }
            if (trustStoreFileName != null && trustStorePassword == null && !"pem".equalsIgnoreCase(trustStoreType)) {
               throw HOTROD.missingTrustStorePassword(trustStoreFileName);
            }
         } else {
            if (keyStoreFileName != null || trustStoreFileName != null) {
               throw HOTROD.xorSSLContext();
            }
         }
         if (hostnameValidation && sniHostName == null) {
            throw HOTROD.missingSniHostName();
         }
      }
   }

   @Override
   public SslConfiguration create() {
      return new SslConfiguration(enabled,
            keyStoreFileName, keyStoreType, keyStorePassword, keyAlias,
            sslContext,
            trustStoreFileName, trustStorePath, trustStoreType, trustStorePassword,
            sniHostName, provider, protocol, ciphers, hostnameValidation);
   }

   @Override
   public SslConfigurationBuilder read(SslConfiguration template, Combine combine) {
      this.enabled = template.enabled();
      this.keyStoreFileName = template.keyStoreFileName();
      this.keyStoreType = template.keyStoreType();
      this.keyStorePassword = template.keyStorePassword();
      this.keyAlias = template.keyAlias();
      this.sslContext = template.sslContext();
      this.trustStoreFileName = template.trustStoreFileName();
      this.trustStorePath = template.trustStorePath();
      this.trustStoreType = template.trustStoreType();
      this.trustStorePassword = template.trustStorePassword();
      this.sniHostName = template.sniHostName();
      this.provider = template.provider();
      this.protocol = template.protocol();
      this.ciphers = template.ciphers() != null ? new ArrayList<>(template.ciphers()) : null;
      this.hostnameValidation = template.hostnameValidation();
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

      if (typed.containsKey(ConfigurationProperties.KEY_ALIAS))
         this.keyAlias(typed.getProperty(ConfigurationProperties.KEY_ALIAS, null, true));

      if (typed.containsKey(ConfigurationProperties.TRUST_STORE_FILE_NAME))
         this.trustStoreFileName(typed.getProperty(ConfigurationProperties.TRUST_STORE_FILE_NAME, trustStoreFileName, true));

      if (typed.containsKey(ConfigurationProperties.TRUST_STORE_TYPE))
         this.trustStoreType(typed.getProperty(ConfigurationProperties.TRUST_STORE_TYPE, null, true));

      if (typed.containsKey(ConfigurationProperties.TRUST_STORE_PASSWORD))
         this.trustStorePassword(typed.getProperty(ConfigurationProperties.TRUST_STORE_PASSWORD, null, true).toCharArray());

      if(typed.containsKey(ConfigurationProperties.SSL_PROTOCOL))
         this.protocol(typed.getProperty(ConfigurationProperties.SSL_PROTOCOL, null, true));

      if(typed.containsKey(ConfigurationProperties.SSL_PROVIDER))
         this.provider(typed.getProperty(ConfigurationProperties.SSL_PROVIDER, null, true));

      if(typed.containsKey(ConfigurationProperties.SSL_CIPHERS))
         this.ciphers(typed.getProperty(ConfigurationProperties.SSL_CIPHERS, null, true).split(" "));

      if (typed.containsKey(ConfigurationProperties.SNI_HOST_NAME))
         this.sniHostName(typed.getProperty(ConfigurationProperties.SNI_HOST_NAME, null, true));

      if (typed.containsKey(ConfigurationProperties.SSL_CONTEXT))
         this.sslContext((SSLContext) typed.get(ConfigurationProperties.SSL_CONTEXT));

      if (typed.containsKey(ConfigurationProperties.USE_SSL))
         this.enabled(typed.getBooleanProperty(ConfigurationProperties.USE_SSL, enabled, true));

      if (typed.containsKey(ConfigurationProperties.SSL_HOSTNAME_VALIDATION))
         this.hostnameValidation(typed.getBooleanProperty(ConfigurationProperties.SSL_HOSTNAME_VALIDATION, hostnameValidation, true));

      return builder.getBuilder();
   }
}
