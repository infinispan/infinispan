package org.infinispan.server.core.configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.configuration.Builder;

/**
 *
 * SSLConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @author Sebastian Łaskawiec
 * @since 5.3
 */
public class SslConfigurationBuilder<T extends ProtocolServerConfiguration, S extends ProtocolServerConfigurationChildBuilder<T, S>>
      extends AbstractProtocolServerConfigurationChildBuilder<T, S>
      implements Builder<SslConfiguration> {
   private boolean enabled = false;
   private boolean requireClientAuth = false;
   private SslEngineConfigurationBuilder defaultDomainConfigurationBuilder = new SslEngineConfigurationBuilder(this);
   private Map<String, SslEngineConfigurationBuilder> sniDomains;

   public SslConfigurationBuilder(ProtocolServerConfigurationChildBuilder<T, S> builder) {
      super(builder);
      sniDomains = new HashMap<>();
      defaultDomainConfigurationBuilder = new SslEngineConfigurationBuilder(this);
      sniDomains.put(SslConfiguration.DEFAULT_SNI_DOMAIN, defaultDomainConfigurationBuilder);
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
    * Enables client certificate authentication
    */
   public SslConfigurationBuilder requireClientAuth(boolean requireClientAuth) {
      this.requireClientAuth = requireClientAuth;
      return this;
   }

   /**
    * Returns SNI domain configuration.
    *
    * @param domain A domain which will hold configuration details. It is also possible to specify <code>*</code>
    *               for all domains.
    * @return {@link SslConfigurationBuilder} instance associated with specified domain.
     */
   public SslEngineConfigurationBuilder sniHostName(String domain) {
      return sniDomains.computeIfAbsent(domain, (v) -> new SslEngineConfigurationBuilder(this));
   }

   /**
    * Sets the {@link SSLContext} to use for setting up SSL connections.
    */
   public SslConfigurationBuilder sslContext(SSLContext sslContext) {
      defaultDomainConfigurationBuilder.sslContext(sslContext);
      return this;
   }

   /**
    * Specifies the filename of a keystore to use to create the {@link SSLContext} You also need to
    * specify a {@link #keyStorePassword(char[])}. Alternatively specify prebuilt {@link SSLContext}
    * through {@link #sslContext(SSLContext)}.
    */
   public SslConfigurationBuilder keyStoreFileName(String keyStoreFileName) {
      defaultDomainConfigurationBuilder.keyStoreFileName(keyStoreFileName);
      return this;
   }

   /**
    * Specifies the type of the keystore, such as JKS or JCEKS. Defaults to JKS
    */
   public SslConfigurationBuilder keyStoreType(String keyStoreType) {
      defaultDomainConfigurationBuilder.keyStoreType(keyStoreType);
      return this;
   }

   /**
    * Specifies the password needed to open the keystore You also need to specify a
    * {@link #keyStoreFileName(String)}. Alternatively specify prebuilt {@link SSLContext}
    * through {@link #sslContext(SSLContext)}.
    */
   public SslConfigurationBuilder keyStorePassword(char[] keyStorePassword) {
      defaultDomainConfigurationBuilder.keyStorePassword(keyStorePassword);
      return this;
   }

   /**
    * Specifies the password needed to access private key associated with certificate stored in specified
    * {@link #keyStoreFileName(String)}. If password is not specified, the password provided in
    * {@link #keyStorePassword(char[])} will be used.
    */
   public SslConfigurationBuilder keyStoreCertificatePassword(char[] keyStoreCertificatePassword) {
      defaultDomainConfigurationBuilder.keyStoreCertificatePassword(keyStoreCertificatePassword);
      return this;
   }


   /**
    * Selects a specific key to choose from the keystore
    */
   public SslConfigurationBuilder keyAlias(String keyAlias) {
      defaultDomainConfigurationBuilder.keyAlias(keyAlias);
      return this;
   }

   /**
    * Specifies the filename of a truststore to use to create the {@link SSLContext} You also need
    * to specify a {@link #trustStorePassword(char[])}. Alternatively specify prebuilt {@link SSLContext}
    * through {@link #sslContext(SSLContext)}.
    */
   public SslConfigurationBuilder trustStoreFileName(String trustStoreFileName) {
      defaultDomainConfigurationBuilder.trustStoreFileName(trustStoreFileName);
      return this;
   }

   /**
    * Specifies the type of the truststore, such as JKS or JCEKS. Defaults to JKS
    */
   public SslConfigurationBuilder trustStoreType(String trustStoreType) {
      defaultDomainConfigurationBuilder.trustStoreType(trustStoreType);
      return this;
   }

   /**
    * Specifies the password needed to open the truststore You also need to specify a
    * {@link #trustStoreFileName(String)}. Alternatively specify prebuilt {@link SSLContext}
    * through {@link #sslContext(SSLContext)}.
    */
   public SslConfigurationBuilder trustStorePassword(char[] trustStorePassword) {
      defaultDomainConfigurationBuilder.trustStorePassword(trustStorePassword);
      return this;
   }

   /**
    * Configures the secure socket protocol.
    *
    * @see javax.net.ssl.SSLContext#getInstance(String)
    * @param protocol The standard name of the requested protocol, e.g TLSv1.2
    */
   public SslConfigurationBuilder protocol(String protocol) {
      defaultDomainConfigurationBuilder.protocol(protocol);
      return this;
   }

   @Override
   public void validate() {
      if (enabled) {
         sniDomains.forEach((domainName, config) -> config.validate());
      }
   }

   @Override
   public SslConfiguration create() {
      Map<String, SslEngineConfiguration> producedSniConfigurations = sniDomains.entrySet()
              .stream()
              .collect(Collectors.toMap(Map.Entry::getKey,
                      e -> e.getValue().create()));
      return new SslConfiguration(enabled, requireClientAuth, producedSniConfigurations);
   }

   @Override
   public SslConfigurationBuilder read(SslConfiguration template) {
      this.enabled = template.enabled();
      this.requireClientAuth = template.requireClientAuth();

      this.sniDomains = new HashMap<>();
      template.sniDomainsConfiguration().entrySet()
              .forEach(e -> sniDomains.put(e.getKey(), new SslEngineConfigurationBuilder(this).read(e.getValue())));

      this.defaultDomainConfigurationBuilder = sniDomains
              .computeIfAbsent(SslConfiguration.DEFAULT_SNI_DOMAIN, (v) -> new SslEngineConfigurationBuilder(this));
      return this;
   }

   @Override
   public S self() {
      return (S) this;
   }
}
