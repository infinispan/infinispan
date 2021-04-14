package org.infinispan.persistence.remote.configuration;

import static org.infinispan.persistence.remote.configuration.SslConfiguration.ENABLED;
import static org.infinispan.persistence.remote.configuration.SslConfiguration.PROTOCOL;
import static org.infinispan.persistence.remote.configuration.SslConfiguration.SNI_HOSTNAME;
import static org.infinispan.persistence.remote.configuration.SslConfiguration.SSL_CONTEXT;

import java.util.List;

import javax.net.ssl.SSLContext;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 *
 * SSLConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class SslConfigurationBuilder extends AbstractSecurityConfigurationChildBuilder implements Builder<SslConfiguration> {
   private static final Log log = LogFactory.getLog(SslConfigurationBuilder.class);

   private KeyStoreConfigurationBuilder keyStoreConfigurationBuilder;
   private TrustStoreConfigurationBuilder trustStoreConfigurationBuilder;

   SslConfigurationBuilder(SecurityConfigurationBuilder builder) {
      super(builder, SslConfiguration.attributeDefinitionSet());
      this.keyStoreConfigurationBuilder = new KeyStoreConfigurationBuilder(builder);
      this.trustStoreConfigurationBuilder = new TrustStoreConfigurationBuilder(builder);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Disables the SSL support
    */
   public SslConfigurationBuilder disable() {
      this.attributes.attribute(ENABLED).set(false);
      return this;
   }

   /**
    * Enables the SSL support
    */
   public SslConfigurationBuilder enable() {
      this.attributes.attribute(ENABLED).set(true);
      return this;
   }

   /**
    * Enables or disables the SSL support
    */
   public SslConfigurationBuilder enabled(boolean enabled) {
      this.attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   /**
    * Specifies the filename of a keystore to use to create the {@link SSLContext} You also need to
    * specify a {@link #keyStorePassword(char[])}. Alternatively specify an initialized {@link #sslContext(SSLContext)}
    */
   public SslConfigurationBuilder keyStoreFileName(String keyStoreFileName) {
      keyStoreConfigurationBuilder.keyStoreFileName(keyStoreFileName);
      return this;
   }

   /**
    * Specifies the type of the keystore, such as JKS or JCEKS. Defaults to JKS
    */
   public SslConfigurationBuilder keyStoreType(String keyStoreType) {
      keyStoreConfigurationBuilder.keyStoreType(keyStoreType);
      return this;
   }

   /**
    * Specifies the password needed to open the keystore You also need to specify a
    * {@link #keyStoreFileName(String)}. Alternatively specify an initialized {@link #sslContext(SSLContext)}
    */
   public SslConfigurationBuilder keyStorePassword(char[] keyStorePassword) {
      keyStoreConfigurationBuilder.keyStorePassword(keyStorePassword);
      return this;
   }

   /**
    * Specifies the password needed to access private key associated with certificate stored in specified
    * {@link #keyStoreFileName(String)}. If password is not specified, password provided in
    * {@link #keyStorePassword(char[])} will be used.
    */
   public SslConfigurationBuilder keyStoreCertificatePassword(char[] keyStoreCertificatePassword) {
      keyStoreConfigurationBuilder.keyStoreCertificatePassword(keyStoreCertificatePassword);
      return this;
   }

   public SslConfigurationBuilder keyAlias(String keyAlias) {
      keyStoreConfigurationBuilder.keyAlias(keyAlias);
      return this;
   }

   public SslConfigurationBuilder sslContext(SSLContext sslContext) {
      this.attributes.attribute(SSL_CONTEXT).set(sslContext);
      return this;
   }

   /**
    * Specifies the filename of a truststore to use to create the {@link SSLContext} You also need
    * to specify a {@link #trustStorePassword(char[])}. Alternatively specify an initialized {@link #sslContext(SSLContext)}
    */
   public SslConfigurationBuilder trustStoreFileName(String trustStoreFileName) {
      trustStoreConfigurationBuilder.trustStoreFileName(trustStoreFileName);
      return this;
   }

   /**
    * Specifies the type of the truststore, such as JKS or JCEKS. Defaults to JKS
    */
   public SslConfigurationBuilder trustStoreType(String trustStoreType) {
      trustStoreConfigurationBuilder.trustStoreType(trustStoreType);
      return this;
   }

   /**
    * Specifies the password needed to open the truststore You also need to specify a
    * {@link #trustStoreFileName(String)}. Alternatively specify an initialized {@link #sslContext(SSLContext)}
    */
   public SslConfigurationBuilder trustStorePassword(char[] trustStorePassword) {
      trustStoreConfigurationBuilder.trustStorePassword(trustStorePassword);
      return this;
   }

   /**
    * Specifies the TLS SNI hostname for the connection
    * @see javax.net.ssl.SSLParameters#setServerNames(List)
     */
   public SslConfigurationBuilder sniHostName(String sniHostName) {
      this.attributes.attribute(SNI_HOSTNAME).set(sniHostName);
      return this;
   }

   /**
    * Configures the secure socket protocol.
    *
    * @see SSLContext#getInstance(String)
    * @param protocol The standard name of the requested protocol, e.g TLSv1.2
    */
   public SslConfigurationBuilder protocol(String protocol) {
      this.attributes.attribute(PROTOCOL).set(protocol);
      return this;
   }

   @Override
   public void validate() {
      // validation will be performed by the RemoteCacheManager configuration builder
   }

   @Override
   public SslConfiguration create() {
      return new SslConfiguration(attributes.protect(), keyStoreConfigurationBuilder.create(), trustStoreConfigurationBuilder.create());
   }

   @Override
   public SslConfigurationBuilder read(SslConfiguration template) {
      this.attributes.read(template.attributes());
      this.keyStoreConfigurationBuilder.read(template.keyStoreConfiguration());
      this.trustStoreConfigurationBuilder.read(template.trustStoreConfiguration());
      return this;
   }
}
