package org.infinispan.rest.helper;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
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

   public RestServerHelper(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
      restServerConfigurationBuilder.host("localhost").corsAllowForLocalhost("http", 80).port(0).maxContentLength(1_000_000);
   }

   public static RestServerHelper defaultRestServer(String... cachesDefined) {
      return defaultRestServer(new ConfigurationBuilder(), cachesDefined);
   }

   public static RestServerHelper defaultRestServer(ConfigurationBuilder configuration, String... cachesDefined) {
      GlobalConfigurationBuilder globalConfigurationBuilder = new GlobalConfigurationBuilder();
      globalConfigurationBuilder.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      GlobalConfigurationBuilder globalConfiguration = globalConfigurationBuilder.nonClusteredDefault();
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

   public RestServerHelper start(String name) {
      restServerConfigurationBuilder.name(name);
      restServer.setAuthenticator(authenticator);
      restServer.start(restServerConfigurationBuilder.build(), cacheManager);
      return this;
   }

   public void clear() {
      InternalCacheRegistry registry = cacheManager.getGlobalComponentRegistry()
            .getComponent(InternalCacheRegistry.class);
      cacheManager.getCacheNames().stream()
            .filter(cacheName -> !registry.isInternalCache(cacheName))
            .forEach(cacheName -> cacheManager.getCache(cacheName).getAdvancedCache().getDataContainer().clear());
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

   public void ignoreCache(String cacheName) {
      restServer.ignoreCache(cacheName);
   }

   public void unignoreCache(String cacheName) {
      restServer.unignore(cacheName);
   }
}
