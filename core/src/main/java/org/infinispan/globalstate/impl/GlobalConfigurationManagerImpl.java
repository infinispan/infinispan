package org.infinispan.globalstate.impl;

import static org.infinispan.util.concurrent.CompletableFutures.uncheckedAwait;
import static org.infinispan.util.logging.Log.CONFIG;

import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

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
import org.infinispan.globalstate.ScopeFilter;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.concurrent.CompletableFutures;
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
   private static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   public static final String CACHE_SCOPE = "cache";

   @Inject EmbeddedCacheManager cacheManager;
   @Inject LocalTopologyManager localTopologyManager;
   @Inject ConfigurationManager configurationManager;
   @Inject InternalCacheRegistry internalCacheRegistry;
   @Inject GlobalComponentRegistry globalComponentRegistry;
   @Inject @ComponentName(KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR)
   ExecutorService executorService;

   private Cache<ScopedState, Object> stateCache;
   private ParserRegistry parserRegistry;
   private LocalConfigurationStorage localConfigurationManager;

   @Start
   void start() {
      switch(configurationManager.getGlobalConfiguration().globalState().configurationStorage()) {
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
            this.localConfigurationManager = configurationManager.getGlobalConfiguration().globalState().configurationStorageClass().get();
            break;
      }

      internalCacheRegistry.registerInternalCache(
            CONFIG_STATE_CACHE_NAME,
            new ConfigurationBuilder().build(),
            EnumSet.of(InternalCacheRegistry.Flag.GLOBAL));
      parserRegistry = new ParserRegistry();

      localConfigurationManager.initialize(cacheManager, configurationManager, executorService);
      // Initialize caches which are present in the initial state. We do this before installing the listener.
      for (Map.Entry<ScopedState, Object> e : getStateCache().entrySet()) {
         if (CACHE_SCOPE.equals(e.getKey().getScope()))
            uncheckedAwait(createCacheLocally(e.getKey().getName(), (CacheState) e.getValue()));
      }
      // Install the listener
      GlobalConfigurationStateListener stateCacheListener = new GlobalConfigurationStateListener(this);
      getStateCache().addListener(stateCacheListener, new ScopeFilter(CACHE_SCOPE));

      // Tell the LocalConfigurationManager that it can load any persistent caches

      List<CompletableFuture<Configuration>> all = localConfigurationManager.loadAll().entrySet().stream().map((entry) ->
            // The cache configuration was permanent, it still needs to be
            getOrCreateCache(entry.getKey(), entry.getValue(), EnumSet.noneOf(CacheContainerAdmin.AdminFlag.class))
      ).collect(Collectors.toList());
      uncheckedAwait(CompletableFutures.sequence(all));
   }

   public Cache<ScopedState, Object> getStateCache() {
      if (stateCache == null) {
         stateCache = cacheManager.getCache(CONFIG_STATE_CACHE_NAME);
      }
      return stateCache;
   }

   @Override
   public CompletableFuture<Configuration> createCache(String cacheName, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
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
      log.debugf("Create cache %s", name);
      ConfigurationBuilderHolder builderHolder = parserRegistry.parse(state.getConfiguration());
      Configuration configuration = builderHolder.getNamedConfigurationBuilders().get(name).build();
      return localConfigurationManager.createCache(name, state.getTemplate(), configuration, state.getFlags());
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
