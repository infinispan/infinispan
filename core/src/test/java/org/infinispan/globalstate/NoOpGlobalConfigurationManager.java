package org.infinispan.globalstate;

import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.CompletableFutures;

/*
 * A no-op implementation for tests which mess up with initial state transfer and RPCs
 */
public class NoOpGlobalConfigurationManager implements GlobalConfigurationManager {
   @Override
   public CompletableFuture<Configuration> createCache(String cacheName, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletableFuture<Configuration> getOrCreateCache(String cacheName, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletableFuture<Configuration> createCache(String cacheName, String template, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletableFuture<Configuration> getOrCreateCache(String cacheName, String template, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletableFuture<Void> removeCache(String cacheName, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return CompletableFutures.completedNull();
   }

   public static void amendCacheManager(EmbeddedCacheManager cm) {
      TestingUtil.replaceComponent(cm, GlobalConfigurationManager.class, new NoOpGlobalConfigurationManager(), true);
   }
}
