package org.infinispan.rest.helper;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.rest.authentication.impl.VoidAuthenticator;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;

/**
 * A small utility class which helps managing REST server.
 *
 * @author Sebastian Łaskawiec
 */
public class RestServerHelper {

   private final EmbeddedCacheManager cacheManager;
   private final RestServer restServer = new RestServer();
   private final RestServerConfigurationBuilder restServerConfigurationBuilder = new RestServerConfigurationBuilder();

   private Authenticator authenticator = new VoidAuthenticator();
   private String host;

   private RestServerHelper(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
      restServerConfigurationBuilder.host("localhost").port(0).maxContentLength(1_000_000);
   }

   public static RestServerHelper defaultRestServer(String... cachesDefined) {
      GlobalConfigurationBuilder globalConfiguration = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalConfiguration.globalJmxStatistics().allowDuplicateDomains(true);
      ConfigurationBuilder configuration = new ConfigurationBuilder();
      DefaultCacheManager cacheManager = new DefaultCacheManager(globalConfiguration.build(), configuration.build());
      for (String cacheConfiguration : cachesDefined) {
         cacheManager.defineConfiguration(cacheConfiguration, configuration.build());
      }

      return new RestServerHelper(cacheManager);
   }

   public RestServerHelper withAuthenticator(Authenticator authenticator) {
      this.authenticator = authenticator;
      return this;
   }

   public void defineCache(String cacheName, ConfigurationBuilder configurationBuilder) {
      cacheManager.defineConfiguration(cacheName, configurationBuilder.build());
   }

   public RestServerHelper start() {
      restServer.setAuthenticator(authenticator);
      restServer.start(restServerConfigurationBuilder.build(), cacheManager);
      return this;
   }

   public void clear() {
      for (String cacheName : cacheManager.getCacheNames()) {
         cacheManager.getCache(cacheName).clear();
      }
   }

   public void stop() {
      restServer.stop();
      cacheManager.stop();
   }

   public int getPort() {
      return restServer.getPort();
   }

   public EmbeddedCacheManager getCacheManager() {
      return cacheManager;
   }

   public RestServerHelper withKeyStore(String keyStorePath, String secret) {
      restServerConfigurationBuilder.ssl().enable();
      restServerConfigurationBuilder.ssl().keyStoreFileName(keyStorePath).keyStorePassword(secret.toCharArray());
      return this;
   }

   public RestServerHelper withTrustStore(String trustStorePath, String secret) {
      restServerConfigurationBuilder.ssl().enable();
      restServerConfigurationBuilder.ssl().trustStoreFileName(trustStorePath).trustStorePassword(secret.toCharArray());
      return this;
   }

   public RestServerHelper withClientAuth() {
      restServerConfigurationBuilder.ssl().enable();
      restServerConfigurationBuilder.ssl().requireClientAuth(true);
      return this;
   }

   public String getHost() {
      return restServer.getHost();
   }
}
