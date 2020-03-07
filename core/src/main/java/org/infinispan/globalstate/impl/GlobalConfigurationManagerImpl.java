package org.infinispan.globalstate.impl;

import static org.infinispan.util.logging.Log.CONFIG;

import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.globalstate.LocalConfigurationStorage;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Implementation of {@link GlobalConfigurationManager}
 *
 * @author Tristan Tarrant
 * @since 9.2
 */
@Scope(Scopes.GLOBAL)
public class GlobalConfigurationManagerImpl implements GlobalConfigurationManager {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   public static final String CACHE_SCOPE = "cache";

   @Inject EmbeddedCacheManager cacheManager;
   @Inject LocalTopologyManager localTopologyManager;
   @Inject ConfigurationManager configurationManager;
   @Inject InternalCacheRegistry internalCacheRegistry;
   @Inject GlobalComponentRegistry globalComponentRegistry;
   @Inject @ComponentName(KnownComponentNames.BLOCKING_EXECUTOR)
   ExecutorService blockingExecutor;

   private Cache<ScopedState, Object> stateCache;
   private ParserRegistry parserRegistry;
   private LocalConfigurationStorage localConfigurationManager;

   @Start
   void start() {
      switch (configurationManager.getGlobalConfiguration().globalState().configurationStorage()) {
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
            this.localConfigurationManager =
                  configurationManager.getGlobalConfiguration().globalState().configurationStorageClass().get();
            break;
      }

      internalCacheRegistry.registerInternalCache(
            CONFIG_STATE_CACHE_NAME,
            new ConfigurationBuilder().build(),
            EnumSet.of(InternalCacheRegistry.Flag.GLOBAL));
      parserRegistry = new ParserRegistry();

      // Start the static caches first
      Set<String> staticCacheNames = new HashSet<>(configurationManager.getDefinedCaches());
      log.debugf("Starting statically defined caches: %s", staticCacheNames);
      for (String cacheName : configurationManager.getDefinedCaches()) {
         SecurityActions.getCache(cacheManager, cacheName);
      }

      localConfigurationManager.initialize(cacheManager, configurationManager, blockingExecutor);

      // Load any state we previously had in the local persistent state into the global state
      Map<String, Configuration> persistedConfigurations = localConfigurationManager.loadAll();

      // Install the global state listener
      GlobalConfigurationStateListener stateCacheListener = new GlobalConfigurationStateListener(this);
      getStateCache().addListener(stateCacheListener);

      // Create all the caches that exist in the global state
      getStateCache().forEach((key, v) -> {
         if (CACHE_SCOPE.equals(key.getScope())) {
            String cacheName = key.getName();
            CacheState cacheState = (CacheState) v;
            Configuration persisted = persistedConfigurations.get(cacheName);
            if (persisted != null) {
               Configuration configuration = buildConfiguration(cacheName, cacheState);
               if (!persisted.matches(configuration)) {
                  throw CONFIG.incompatibleClusterConfiguration(cacheName, configuration, persisted);
               } else {
                  // The cache configuration matches, we can skip it when iterating the ones from the local state
                  persistedConfigurations.remove(cacheName);
               }
            }
            CompletionStages.join(createCacheLocally(cacheName, cacheState));
         }
      });

      // Create caches that are in the local persistent state but not in the global state
      persistedConfigurations.forEach((cacheName, configuration) -> {
         Configuration staticConfiguration = cacheManager.getCacheConfiguration(cacheName);
         if (staticConfiguration != null) {
            if (!staticConfiguration.matches(configuration)) {
               throw CONFIG.incompatiblePersistedConfiguration(cacheName, configuration, staticConfiguration);
            } else {
               // The cache configuration matches, we can skip it when iterating the ones from the local state
               persistedConfigurations.remove(cacheName);
            }
         }

         // The cache configuration was permanent, it still needs to be
         EnumSet<CacheContainerAdmin.AdminFlag> adminFlags = EnumSet.noneOf(CacheContainerAdmin.AdminFlag.class);
         CompletionStages.join(getOrCreateCache(cacheName, configuration, adminFlags));
      });
   }

   public Cache<ScopedState, Object> getStateCache() {
      if (stateCache == null) {
         stateCache = cacheManager.getCache(CONFIG_STATE_CACHE_NAME);
      }
      return stateCache;
   }

   @Override
   public CompletableFuture<Configuration> createCache(String cacheName, Configuration configuration,
                                                       EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      if (cacheManager.cacheExists(cacheName)) {
         throw CONFIG.cacheExists(cacheName);
      } else {
         return getOrCreateCache(cacheName, configuration, flags);
      }
   }

   @Override
   public CompletableFuture<Configuration> getOrCreateCache(String cacheName, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return createCache(cacheName, null, configuration, flags);
   }

   @Override
   public CompletableFuture<Configuration> createCache(String cacheName, String template, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      if (cacheManager.cacheExists(cacheName)) {
         throw CONFIG.cacheExists(cacheName);
      } else {
         return getOrCreateCache(cacheName, template, flags);
      }
   }

   @Override
   public CompletableFuture<Configuration> getOrCreateCache(String cacheName, String template, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      Configuration configuration;
      if (template == null) {
         // The user has not specified a template, if a cache already exists just return it without checking for compatibility
         if (cacheManager.cacheExists(cacheName))
            return CompletableFuture.completedFuture(configurationManager.getConfiguration(cacheName, true));
         else {
            Optional<String> defaultCacheName = configurationManager.getGlobalConfiguration().defaultCacheName();
            if (defaultCacheName.isPresent()) {
               configuration = configurationManager.getConfiguration(defaultCacheName.get(), true);
            } else {
               configuration = null;
            }
         }
         if (configuration == null) {
            configuration = new ConfigurationBuilder().build();
         }
      } else {
         configuration = configurationManager.getConfiguration(template, true);
         if (configuration == null) {
            throw CONFIG.undeclaredConfiguration(template, cacheName);
         }
      }
      return createCache(cacheName, template, configuration, flags);
   }

   CompletableFuture<Configuration> createCache(String cacheName, String template, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      localConfigurationManager.validateFlags(flags);
      try {
         CacheState state = new CacheState(template, parserRegistry.serialize(cacheName, configuration), flags);
         return getStateCache().putIfAbsentAsync(new ScopedState(CACHE_SCOPE, cacheName), state).thenApply((v) -> configuration);
      } catch (Exception e) {
         throw CONFIG.configurationSerializationFailed(cacheName, configuration, e);
      }
   }

   CompletableFuture<Void> createCacheLocally(String name, CacheState state) {
      log.debugf("Starting cache %s from global state", name);
      Configuration configuration = buildConfiguration(name, state);
      return localConfigurationManager.createCache(name, state.getTemplate(), configuration, state.getFlags());
   }

   private Configuration buildConfiguration(String name, CacheState state) {
      ConfigurationBuilderHolder builderHolder = parserRegistry.parse(state.getConfiguration());
      return builderHolder.getNamedConfigurationBuilders().get(name).build(configurationManager.getGlobalConfiguration());
   }

   @Override
   public CompletableFuture<Void> removeCache(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      ScopedState cacheScopedState = new ScopedState(CACHE_SCOPE, name);
      if (getStateCache().containsKey(cacheScopedState)) {
         try {
            localTopologyManager.setCacheRebalancingEnabled(name, false);
         } catch (Exception e) {
            // Ignore
         }
         return getStateCache().removeAsync(cacheScopedState).thenCompose((r) -> CompletableFutures.completedNull());
      } else {
         return localConfigurationManager.removeCache(name, flags);
      }
   }

   CompletableFuture<Void> removeCacheLocally(String name, CacheState state) {
      return localConfigurationManager.removeCache(name, state.getFlags());
   }
}
