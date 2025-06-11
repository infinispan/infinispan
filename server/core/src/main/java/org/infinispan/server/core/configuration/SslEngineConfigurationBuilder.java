package org.infinispan.server.core.configuration;

import java.util.function.Supplier;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.InstanceSupplier;
import org.infinispan.server.core.logging.Log;
import org.infinispan.util.logging.LogFactory;

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
   private String keyAlias;
   private String protocol;
   private Supplier<SSLContext> sslContextSupplier;
   private String trustStoreFileName;
   private char[] trustStorePassword;
   private String keyStoreType;
   private String trustStoreType;

   SslEngineConfigurationBuilder(SslConfigurationBuilder parentSslConfigurationBuilder) {
      this.parentSslConfigurationBuilder = parentSslConfigurationBuilder;
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
   }

   /**
    * Sets the {@link SSLContext} to use for setting up SSL connections.
    */
   public SslEngineConfigurationBuilder sslContext(SSLContext sslContext) {
      this.sslContextSupplier = new InstanceSupplier<>(sslContext);
      return this;
   }

   /**
    * Sets the {@link SSLContext} to use for setting up SSL connections.
    */
   public SslEngineConfigurationBuilder sslContext(Supplier<SSLContext> sslContext) {
      this.sslContextSupplier = sslContext;
      return this;
   }

   /**
    * Specifies the filename of a keystore to use to create the {@link SSLContext} You also need to
    * specify a {@link #keyStorePassword(char[])}. Alternatively specify an initialized {@link #sslContext(SSLContext)}.
    */
   public SslEngineConfigurationBuilder keyStoreFileName(String keyStoreFileName) {
      this.keyStoreFileName = keyStoreFileName;
      return this;
   }

   /**
    * Specifies the type of the keystore, such as JKS or JCEKS. Defaults to JKS
    */
   public SslEngineConfigurationBuilder keyStoreType(String keyStoreType) {
      this.keyStoreType = keyStoreType;
      return this;
   }

   /**
    * Specifies the password needed to open the keystore You also need to specify a
    * {@link #keyStoreFileName(String)}. Alternatively specify an initialized {@link #sslContext(SSLContext)}.
    */
   public SslEngineConfigurationBuilder keyStorePassword(char[] keyStorePassword) {
      this.keyStorePassword = keyStorePassword;
      return this;
   }

   /**
    * Specifies the filename of a truststore to use to create the {@link SSLContext} You also need
    * to specify a {@link #trustStorePassword(char[])}. Alternatively specify an initialized {@link #sslContext(SSLContext)}.
    */
   public SslEngineConfigurationBuilder trustStoreFileName(String trustStoreFileName) {
      this.trustStoreFileName = trustStoreFileName;
      return this;
   }

   /**
    * Specifies the type of the truststore, such as JKS or JCEKS. Defaults to JKS
    */
   public SslEngineConfigurationBuilder trustStoreType(String trustStoreType) {
      this.trustStoreType = trustStoreType;
      return this;
   }

   /**
    * Specifies the password needed to open the truststore You also need to specify a
    * {@link #trustStoreFileName(String)}. Alternatively specify an initialized {@link #sslContext(SSLContext)}.
    */
   public SslEngineConfigurationBuilder trustStorePassword(char[] trustStorePassword) {
      this.trustStorePassword = trustStorePassword;
      return this;
   }

   /**
    * Selects a specific key to choose from the keystore
    */
   public SslEngineConfigurationBuilder keyAlias(String keyAlias) {
      this.keyAlias = keyAlias;
      return this;
   }

   /**
    * Configures the secure socket protocol.
    *
    * @see javax.net.ssl.SSLContext#getInstance(String)
    * @param protocol The standard name of the requested protocol, e.g TLSv1.2
    */
   public SslEngineConfigurationBuilder protocol(String protocol) {
      this.protocol = protocol;
      return this;
   }

   @Override
   public void validate() {
      if (sslContextSupplier == null || sslContextSupplier.get() == null) {
         if (keyStoreFileName == null) {
            throw log.noSSLKeyManagerConfiguration();
         }
         if (keyStorePassword == null) {
            throw log.missingKeyStorePassword(keyStoreFileName);
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
      return new SslEngineConfiguration(keyStoreFileName, keyStoreType, keyStorePassword, keyAlias, sslContextSupplier, trustStoreFileName, trustStoreType, trustStorePassword, protocol);
   }

   @Override
   public SslEngineConfigurationBuilder read(SslEngineConfiguration template, Combine combine) {
      this.keyStoreFileName = template.keyStoreFileName();
      this.keyStoreType = template.keyStoreType();
      this.keyStorePassword = template.keyStorePassword();
      this.keyAlias = template.keyAlias();
      this.sslContextSupplier = template.sslContextSupplier();
      this.trustStoreFileName = template.trustStoreFileName();
      this.trustStoreType  = template.trustStoreType();
      this.trustStorePassword = template.trustStorePassword();
      this.protocol = template.protocol();
      return this;
   }

   @Override
   public SslEngineConfigurationBuilder sniHostName(String domain) {
      return parentSslConfigurationBuilder.sniHostName(domain);
   }
}
