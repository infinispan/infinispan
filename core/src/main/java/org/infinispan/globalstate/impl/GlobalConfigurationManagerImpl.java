package org.infinispan.globalstate.impl;

import java.io.ByteArrayOutputStream;
import java.lang.invoke.MethodHandles;
import java.util.EnumSet;

import org.infinispan.Cache;
import org.infinispan.commands.RemoveCacheCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.PostStart;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Implementation of {@link GlobalConfigurationManager}
 *
 * @author Tristan Tarrant
 * @since 9.1
 */

public class GlobalConfigurationManagerImpl implements GlobalConfigurationManager {
   private static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private GlobalConfiguration globalConfiguration;
   private EmbeddedCacheManager cacheManager;
   private Cache<String, String> stateCache;
   private GlobalConfigurationStateListener stateCacheListener;
   private TimeService timeService;

   @Inject
   public void inject(GlobalConfiguration globalConfiguration, EmbeddedCacheManager cacheManager,
                      TimeService timeService) {
      this.globalConfiguration = globalConfiguration;
      this.cacheManager = cacheManager;
      this.timeService = timeService;
   }

   @Start
   public void start() {
      InternalCacheRegistry internalCacheRegistry = cacheManager.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      internalCacheRegistry.registerInternalCache(CONFIG_STATE_CACHE_NAME, getStateCacheConfiguration(globalConfiguration).build(), EnumSet.noneOf(InternalCacheRegistry.Flag.class));
   }

   @PostStart
   public void postStart() {
      initializeClusterConfigurations();
      stateCacheListener = new GlobalConfigurationStateListener(this);
      getStateCache().addListener(stateCacheListener);
   }

   private void initializeClusterConfigurations() {
      getStateCache().forEach(this::localCreateCache);
   }

   @Stop
   public void stop() {
      getStateCache().removeListener(stateCacheListener);
   }

   private ConfigurationBuilder getStateCacheConfiguration(GlobalConfiguration globalConfiguration) {
      CacheMode cacheMode = globalConfiguration.isClustered() ? CacheMode.REPL_SYNC : CacheMode.LOCAL;

      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.clustering().cacheMode(cacheMode).sync().stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(false);

      return cfg;
   }

   Cache<String, String> getStateCache() {
      if (stateCache == null) {
         stateCache = cacheManager.getCache(CONFIG_STATE_CACHE_NAME);
      }
      return stateCache;
   }

   @Override
   public Configuration createCache(String cacheName, Configuration configuration) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      try {
         new ParserRegistry().serialize(os, cacheName, configuration);
         getStateCache().put(cacheName, os.toString("UTF-8"));
         return configuration;
      } catch (Exception e) {
         throw log.configurationSerializationFailed(cacheName, configuration, e);
      }
   }

   void localCreateCache(String cacheName, String cacheConfiguration) {
      ConfigurationBuilderHolder builderHolder = new ParserRegistry().parse(cacheConfiguration);
      Configuration configuration = builderHolder.getNamedConfigurationBuilders().get(cacheName).build();
      Configuration existing = cacheManager.getCacheConfiguration(cacheName);
      if (existing == null) {
         cacheManager.defineConfiguration(cacheName, configuration);
         System.err.printf("Defined cache '%s' on '%s'\n", cacheName, cacheManager.getAddress());
      } else if (!existing.equals(configuration)) {
         throw log.incompatibleClusterConfiguration(cacheName, configuration, existing);
      }
      cacheManager.getCache(cacheName);
   }

   @Override
   public void removeCache(String cacheName) {
      getStateCache().remove(cacheName);
   }

   void localRemoveCache(String cacheName) {
      RemoveCacheCommand.removeCache(cacheManager, cacheName);
   }
}
