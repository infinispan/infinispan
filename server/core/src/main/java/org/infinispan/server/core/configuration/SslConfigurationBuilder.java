package org.infinispan.server.core.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.server.core.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * SSLConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @author Sebastian Łaskawiec
 * @since 5.3
 */
public class SslConfigurationBuilder<T extends ProtocolServerConfiguration, S extends ProtocolServerConfigurationChildBuilder<T, S>> implements Builder<SslConfiguration>, ProtocolServerConfigurationChildBuilder<T, S> {
   private static final Log log = LogFactory.getLog(SslConfigurationBuilder.class, Log.class);
   private final ProtocolServerConfigurationChildBuilder<T, S> parentConfigurationBuilder;
   private boolean enabled = false;
   private boolean requireClientAuth = false;
   private SslEngineConfigurationBuilder defaultDomainConfigurationBuilder = new SslEngineConfigurationBuilder(this);
   private Map<String, SslEngineConfigurationBuilder> sniDomains;

   SslConfigurationBuilder(ProtocolServerConfigurationChildBuilder<T, S> parentConfigurationBuilder) {
      this.parentConfigurationBuilder = parentConfigurationBuilder;
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
    * specify a {@link #keyStorePassword(String)}. Alternatively specify an array of
    * {@link #keyManagers(KeyManager[])}
    */
   public SslConfigurationBuilder keyStoreFileName(String keyStoreFileName) {
      defaultDomainConfigurationBuilder.keyStoreFileName(keyStoreFileName);
      return this;
   }

   /**
    * Specifies the password needed to open the keystore You also need to specify a
    * {@link #keyStoreFileName(String)} Alternatively specify an array of
    * {@link #keyManagers(KeyManager[])}
    */
   public SslConfigurationBuilder keyStorePassword(char[] keyStorePassword) {
      defaultDomainConfigurationBuilder.keyStorePassword(keyStorePassword);
      return this;
   }

   /**
    * Specifies the password needed to access private key associated with certificate stored in specified
    * {@link #keyStoreFileName(String)}. If password is not specified, password provided in
    * {@link #keyStorePassword(String)} will be used.
    */
   public SslConfigurationBuilder keyStoreCertificatePassword(char[] keyStoreCertificatePassword) {
      defaultDomainConfigurationBuilder.keyStoreCertificatePassword(keyStoreCertificatePassword);
      return this;
   }

   /**
    * Specifies the filename of a truststore to use to create the {@link SSLContext} You also need
    * to specify a {@link #trustStorePassword(String)}. Alternatively specify an array of
    * {@link #trustManagers(TrustManager[])}
    */
   public SslConfigurationBuilder trustStoreFileName(String trustStoreFileName) {
      defaultDomainConfigurationBuilder.trustStoreFileName(trustStoreFileName);
      return this;
   }

   /**
    * Specifies the password needed to open the truststore You also need to specify a
    * {@link #trustStoreFileName(String)} Alternatively specify an array of
    * {@link #trustManagers(TrustManager[])}
    */
   public SslConfigurationBuilder trustStorePassword(char[] trustStorePassword) {
      defaultDomainConfigurationBuilder.trustStorePassword(trustStorePassword);
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
   public S defaultCacheName(String defaultCacheName) {
      return parentConfigurationBuilder.defaultCacheName(defaultCacheName);
   }

   @Override
   public S name(String name) {
      return parentConfigurationBuilder.name(name);
   }

   @Override
   public S host(String host) {
      return parentConfigurationBuilder.host(host);
   }

   @Override
   public S port(int port) {
      return parentConfigurationBuilder.port(port);
   }

   @Override
   public S idleTimeout(int idleTimeout) {
      return parentConfigurationBuilder.idleTimeout(idleTimeout);
   }

   @Override
   public S tcpNoDelay(boolean tcpNoDelay) {
      return parentConfigurationBuilder.tcpNoDelay(tcpNoDelay);
   }

   @Override
   public S recvBufSize(int recvBufSize) {
      return parentConfigurationBuilder.recvBufSize(recvBufSize);
   }

   @Override
   public S sendBufSize(int sendBufSize) {
      return parentConfigurationBuilder.sendBufSize(sendBufSize);
   }

   @Override
   public SslConfigurationBuilder ssl() {
      return parentConfigurationBuilder.ssl();
   }

   @Override
   public S workerThreads(int workerThreads) {
      return parentConfigurationBuilder.workerThreads(workerThreads);
   }

   @Override
   public S ignoredCaches(Set<String> ignoredCaches) {
      return parentConfigurationBuilder.ignoredCaches(ignoredCaches);
   }

   @Override
   public T build() {
      return parentConfigurationBuilder.build();
   }

   @Override
   public S self() {
      return parentConfigurationBuilder.self();
   }
}
