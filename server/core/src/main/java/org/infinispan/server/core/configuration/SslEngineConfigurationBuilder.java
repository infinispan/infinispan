package org.infinispan.server.core.configuration;

import java.util.function.Supplier;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.InstanceSupplier;
import org.infinispan.server.core.logging.Log;

/**
 *
 * SSLConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class SslEngineConfigurationBuilder implements SslConfigurationChildBuilder {
   private static final Log log = Log.getLog(SslEngineConfigurationBuilder.class);
   private final SslConfigurationBuilder parentSslConfigurationBuilder;
   private final AttributeSet attributes;

   SslEngineConfigurationBuilder(SslConfigurationBuilder parentSslConfigurationBuilder) {
      this.parentSslConfigurationBuilder = parentSslConfigurationBuilder;
      this.attributes = SslEngineConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Sets the {@link SSLContext} to use for setting up SSL connections.
    */
   public SslEngineConfigurationBuilder sslContext(SSLContext sslContext) {
      attributes.attribute(SslEngineConfiguration.SSL_CONTEXT).set(new InstanceSupplier<>(sslContext));
      return this;
   }

   /**
    * Sets the {@link SSLContext} to use for setting up SSL connections.
    */
   public SslEngineConfigurationBuilder sslContext(Supplier<SSLContext> sslContext) {
      attributes.attribute(SslEngineConfiguration.SSL_CONTEXT).set(sslContext);
      return this;
   }

   /**
    * Specifies the filename of a keystore to use to create the {@link SSLContext} You also need to
    * specify a {@link #keyStorePassword(char[])}. Alternatively specify an initialized {@link #sslContext(SSLContext)}.
    */
   public SslEngineConfigurationBuilder keyStoreFileName(String keyStoreFileName) {
      attributes.attribute(SslEngineConfiguration.KEY_STORE_FILE_NAME).set(keyStoreFileName);
      return this;
   }

   /**
    * Specifies the type of the keystore, such as JKS or JCEKS. Defaults to JKS
    */
   public SslEngineConfigurationBuilder keyStoreType(String keyStoreType) {
      attributes.attribute(SslEngineConfiguration.KEY_STORE_TYPE).set(keyStoreType);
      return this;
   }

   /**
    * Specifies the password needed to open the keystore You also need to specify a
    * {@link #keyStoreFileName(String)}. Alternatively specify an initialized {@link #sslContext(SSLContext)}.
    */
   public SslEngineConfigurationBuilder keyStorePassword(char[] keyStorePassword) {
      attributes.attribute(SslEngineConfiguration.KEY_STORE_PASSWORD).set(keyStorePassword);
      return this;
   }

   /**
    * Specifies the filename of a truststore to use to create the {@link SSLContext} You also need
    * to specify a {@link #trustStorePassword(char[])}. Alternatively specify an initialized {@link #sslContext(SSLContext)}.
    */
   public SslEngineConfigurationBuilder trustStoreFileName(String trustStoreFileName) {
      attributes.attribute(SslEngineConfiguration.TRUST_STORE_FILE_NAME).set(trustStoreFileName);
      return this;
   }

   /**
    * Specifies the type of the truststore, such as JKS or JCEKS. Defaults to JKS
    */
   public SslEngineConfigurationBuilder trustStoreType(String trustStoreType) {
      attributes.attribute(SslEngineConfiguration.TRUST_STORE_TYPE).set(trustStoreType);
      return this;
   }

   /**
    * Specifies the password needed to open the truststore You also need to specify a
    * {@link #trustStoreFileName(String)}. Alternatively specify an initialized {@link #sslContext(SSLContext)}.
    */
   public SslEngineConfigurationBuilder trustStorePassword(char[] trustStorePassword) {
      attributes.attribute(SslEngineConfiguration.TRUST_STORE_PASSWORD).set(trustStorePassword);
      return this;
   }

   /**
    * Selects a specific key to choose from the keystore
    */
   public SslEngineConfigurationBuilder keyAlias(String keyAlias) {
      attributes.attribute(SslEngineConfiguration.KEY_ALIAS).set(keyAlias);
      return this;
   }

   /**
    * Configures the secure socket protocol.
    *
    * @see javax.net.ssl.SSLContext#getInstance(String)
    * @param protocol The standard name of the requested protocol, e.g TLSv1.2
    */
   public SslEngineConfigurationBuilder protocol(String protocol) {
      attributes.attribute(SslEngineConfiguration.PROTOCOL).set(protocol);
      return this;
   }

   @Override
   public void validate() {
      Supplier<SSLContext> sslContextSupplier = attributes.attribute(SslEngineConfiguration.SSL_CONTEXT).get();
      if (sslContextSupplier == null || sslContextSupplier.get() == null) {
         String keyStoreFileName = attributes.attribute(SslEngineConfiguration.KEY_STORE_FILE_NAME).get();
         if (keyStoreFileName == null) {
            throw log.noSSLKeyManagerConfiguration();
         }
         if (attributes.attribute(SslEngineConfiguration.KEY_STORE_PASSWORD).get() == null) {
            throw log.missingKeyStorePassword(keyStoreFileName);
         }
         String trustStoreFileName = attributes.attribute(SslEngineConfiguration.TRUST_STORE_FILE_NAME).get();
         if (trustStoreFileName != null && attributes.attribute(SslEngineConfiguration.TRUST_STORE_PASSWORD).get() == null) {
            throw log.missingTrustStorePassword(trustStoreFileName);
         }
      } else {
         if (attributes.attribute(SslEngineConfiguration.KEY_STORE_FILE_NAME).get() != null
               || attributes.attribute(SslEngineConfiguration.TRUST_STORE_FILE_NAME).get() != null) {
            throw log.xorSSLContext();
         }
      }
   }

   @Override
   public SslEngineConfiguration create() {
      return new SslEngineConfiguration(attributes.protect());
   }

   @Override
   public SslEngineConfigurationBuilder read(SslEngineConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public SslEngineConfigurationBuilder sniHostName(String domain) {
      return parentSslConfigurationBuilder.sniHostName(domain);
   }
}
