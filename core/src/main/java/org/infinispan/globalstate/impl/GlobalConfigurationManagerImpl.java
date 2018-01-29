package org.infinispan.globalstate.impl;

import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.PostStart;
import org.infinispan.factories.annotations.Start;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.globalstate.LocalConfigurationStorage;
import org.infinispan.globalstate.ScopeFilter;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
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

   public static final String CACHE_SCOPE = "cache";

   private EmbeddedCacheManager cacheManager;
   private Cache<ScopedState, Object> stateCache;
   private ParserRegistry parserRegistry;
   private LocalConfigurationStorage localConfigurationManager;

   @Inject
   public void inject(GlobalConfiguration globalConfiguration, EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
      switch(globalConfiguration.globalState().configurationStorage()) {
         case IMMUTABLE:
            this.localConfigurationManager = new ImmutableLocalConfigurationStorage();
            break;
         case VOLATILE:
            this.localConfigurationManager = new VolatileLocalConfigurationStorage();
            break;
         case OVERLAY:
            this.localConfigurationManager = new OverlayLocalConfigurationStorage();
            break;
         default:
            this.localConfigurationManager = globalConfiguration.globalState().configurationStorageClass().get();
            break;
      }
   }

   @Start
   public void start() {
      InternalCacheRegistry internalCacheRegistry = cacheManager.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      internalCacheRegistry.registerInternalCache(
            CONFIG_STATE_CACHE_NAME,
            new ConfigurationBuilder().build(),
            EnumSet.of(InternalCacheRegistry.Flag.GLOBAL));
      parserRegistry = new ParserRegistry();
      cacheManager.getGlobalComponentRegistry().wireDependencies(localConfigurationManager);
   }

   @PostStart
   public void postStart() {
      localConfigurationManager.initialize(cacheManager);
      // Initialize caches which are present in the initial state. We do this before installing the listener.
      for (Map.Entry<ScopedState, Object> e : getStateCache().entrySet()) {
         if (CACHE_SCOPE.equals(e.getKey().getScope()))
            createCacheLocally(e.getKey().getName(), (CacheState) e.getValue());
      }
      // Install the listener
      GlobalConfigurationStateListener stateCacheListener = new GlobalConfigurationStateListener(this);
      getStateCache().addListener(stateCacheListener, new ScopeFilter(CACHE_SCOPE));

      // Tell the LocalConfigurationManager that it can load any persistent caches
      localConfigurationManager.loadAll().forEach((name, configuration) -> {
         // The cache configuration was permanent, it still needs to be
         getOrCreateCache(name, configuration, EnumSet.of(CacheContainerAdmin.AdminFlag.PERMANENT));
      });
   }

   public Cache<ScopedState, Object> getStateCache() {
      if (stateCache == null) {
         stateCache = cacheManager.getCache(CONFIG_STATE_CACHE_NAME);
      }
      return stateCache;
   }

   @Override
   public Configuration createCache(String cacheName, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      if (cacheManager.cacheExists(cacheName)) {
         throw log.cacheExists(cacheName);
      } else {
         return getOrCreateCache(cacheName, configuration, flags);
      }
   }

   @Override
   public Configuration getOrCreateCache(String cacheName, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return createCache(cacheName, null, configuration, flags);
   }

   @Override
   public Configuration createCache(String cacheName, String template, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      if (cacheManager.cacheExists(cacheName)) {
         throw log.cacheExists(cacheName);
      } else {
         return getOrCreateCache(cacheName, template, flags);
      }
   }

   @Override
   public Configuration getOrCreateCache(String cacheName, String template, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      Configuration configuration;
      if (template == null) {
         // The user has not specified a template, if a cache already exists just return it without checking for compatibility
         if (cacheManager.cacheExists(cacheName))
            return cacheManager.getCacheConfiguration(cacheName);
         else {
            configuration = cacheManager.getDefaultCacheConfiguration();
         }
         if (configuration == null) {
            configuration = new ConfigurationBuilder().build();
         }
      } else {
         configuration = cacheManager.getCacheConfiguration(template);
         if (configuration == null) {
            throw log.undeclaredConfiguration(template, cacheName);
         }
      }
      return createCache(cacheName, template, configuration, flags);
   }

   Configuration createCache(String cacheName, String template, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      localConfigurationManager.validateFlags(flags);
      try {
         CacheState state = new CacheState(template, parserRegistry.serialize(cacheName, configuration), flags);
         getStateCache().putIfAbsent(new ScopedState(CACHE_SCOPE, cacheName), state);
         return configuration;
      } catch (Exception e) {
         throw log.configurationSerializationFailed(cacheName, configuration, e);
      }
   }

   void createCacheLocally(String name, CacheState state) {
      log.debugf("Create cache %s", name);
      ConfigurationBuilderHolder builderHolder = parserRegistry.parse(state.getConfiguration());
      Configuration configuration = builderHolder.getNamedConfigurationBuilders().get(name).build();
      localConfigurationManager.createCache(name, state.getTemplate(), configuration, state.getFlags());
   }

   @Override
   public void removeCache(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      ScopedState cacheScopedState = new ScopedState(CACHE_SCOPE, name);
      if (getStateCache().containsKey(cacheScopedState)) {
         getStateCache().remove(cacheScopedState);
      } else {
         localConfigurationManager.removeCache(name, flags);
      }
   }

   void removeCacheLocally(String name, CacheState state) {
      localConfigurationManager.removeCache(name, state.getFlags());
   }
}
