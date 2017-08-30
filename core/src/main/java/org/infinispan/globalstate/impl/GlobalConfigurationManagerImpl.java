package org.infinispan.globalstate.impl;

import java.io.ByteArrayOutputStream;
import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commands.RemoveCacheCommand;
import org.infinispan.commons.api.CacheContainerAdmin;
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
import org.infinispan.globalstate.ScopeFilter;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Implementation of {@link GlobalConfigurationManager}
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public class GlobalConfigurationManagerImpl implements GlobalConfigurationManager {
   private static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private GlobalConfiguration globalConfiguration;
   private EmbeddedCacheManager cacheManager;
   private Cache<ScopedState, Object> stateCache;
   private GlobalConfigurationStateListener stateCacheListener;
   private TimeService timeService;
   public static final String CACHE_SCOPE = "cache";

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
      internalCacheRegistry.registerInternalCache(
            CONFIG_STATE_CACHE_NAME,
            new ConfigurationBuilder().build(),
            EnumSet.of(InternalCacheRegistry.Flag.GLOBAL));
   }

   @PostStart
   public void postStart() {
      initializeClusterConfigurations();
      stateCacheListener = new GlobalConfigurationStateListener(this);
      getStateCache().addListener(stateCacheListener, new ScopeFilter(CACHE_SCOPE));
   }

   private void initializeClusterConfigurations() {
      for(Map.Entry<ScopedState, Object> e : getStateCache().getAdvancedCache().entrySet()) {
         if (CACHE_SCOPE.equals(e.getKey().getScope()))
            localCreateCache(e.getKey().getName(), (String)e.getValue());
      }
   }

   public Cache<ScopedState, Object> getStateCache() {
      if (stateCache == null) {
         stateCache = cacheManager.getCache(CONFIG_STATE_CACHE_NAME);
      }
      return stateCache;
   }

   @Override
   public Configuration createCache(String cacheName, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      try {
         new ParserRegistry().serialize(os, cacheName, configuration);
         getStateCache().put(new ScopedState(CACHE_SCOPE, cacheName), os.toString("UTF-8"));
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
   public void removeCache(String cacheName, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      getStateCache().remove(new ScopedState(CACHE_SCOPE, cacheName));
   }

   void localRemoveCache(String cacheName) {
      RemoveCacheCommand.removeCache(cacheManager, cacheName);
   }
}
