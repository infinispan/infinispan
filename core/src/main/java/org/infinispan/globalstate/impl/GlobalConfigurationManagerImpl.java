package org.infinispan.globalstate.impl;

import static org.infinispan.util.logging.Log.CONFIG;

import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
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
import org.infinispan.util.concurrent.BlockingManager;
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
   public static final String TEMPLATE_SCOPE = "template";

   @Inject EmbeddedCacheManager cacheManager;
   @Inject LocalTopologyManager localTopologyManager;
   @Inject ConfigurationManager configurationManager;
   @Inject InternalCacheRegistry internalCacheRegistry;
   @Inject GlobalComponentRegistry globalComponentRegistry;
   @Inject BlockingManager blockingManager;

   private Cache<ScopedState, Object> stateCache;
   private ParserRegistry parserRegistry;
   private LocalConfigurationStorage localConfigurationManager;

   static boolean isKnownScope(String scope) {
      return CACHE_SCOPE.equals(scope) || TEMPLATE_SCOPE.equals(scope);
   }

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

      localConfigurationManager.initialize(cacheManager, configurationManager, blockingManager);

      // Install the global state listener
      GlobalConfigurationStateListener stateCacheListener = new GlobalConfigurationStateListener(this);
      getStateCache().addListener(stateCacheListener);

      Map<String, Configuration> persistedCaches = localConfigurationManager.loadAllCaches();
      Map<String, Configuration> persistedTemplates = localConfigurationManager.loadAllTemplates();

      getStateCache().forEach((key, v) -> {
         String scope = key.getScope();
         if (isKnownScope(scope)) {
            String name = key.getName();
            CacheState state = (CacheState) v;
            boolean cacheScope = CACHE_SCOPE.equals(scope);
            Map<String, Configuration> map = cacheScope ? persistedCaches : persistedTemplates;
            ensureClusterCompatibility(name, state, map);
            CompletableFuture<Void> future = cacheScope ? createCacheLocally(name, state) : createTemplateLocally(name, state);
            CompletionStages.join(future);
         }
      });

      EnumSet<CacheContainerAdmin.AdminFlag> adminFlags = EnumSet.noneOf(CacheContainerAdmin.AdminFlag.class);
      persistedCaches.forEach((name, configuration) -> {
         ensurePersistenceCompatibility(name, configuration, persistedCaches);
         // The cache configuration was permanent, it still needs to be
         CompletionStages.join(getOrCreateCache(name, configuration, adminFlags));
      });

      persistedTemplates.forEach((name, configuration) -> {
         ensurePersistenceCompatibility(name, configuration, persistedTemplates);
         // The template was permanent, it still needs to be
         CompletionStages.join(getOrCreateTemplate(name, configuration, adminFlags));
      });
   }

   private void ensureClusterCompatibility(String name, CacheState state, Map<String, Configuration> configs) {
      Configuration persisted = configs.get(name);
      if (persisted != null) {
         // Template value is not serialized, so buildConfiguration param is irrelevant
         Configuration configuration = CompletionStages.join(buildConfiguration(name, state, false));
         if (!persisted.matches(configuration)) {
            throw CONFIG.incompatibleClusterConfiguration(name, configuration, persisted);
         } else {
            // The configuration matches, we can skip it when iterating the ones from the local state
            configs.remove(name);
         }
      }
   }

   private void ensurePersistenceCompatibility(String name, Configuration configuration, Map<String, Configuration> configs) {
      Configuration staticConfiguration = cacheManager.getCacheConfiguration(name);
      if (staticConfiguration != null) {
         if (!staticConfiguration.matches(configuration)) {
            throw CONFIG.incompatiblePersistedConfiguration(name, configuration, staticConfiguration);
         } else {
            configs.remove(name);
         }
      }
   }

   @Override
   public Cache<ScopedState, Object> getStateCache() {
      if (stateCache == null) {
         stateCache = cacheManager.getCache(CONFIG_STATE_CACHE_NAME);
      }
      return stateCache;
   }

   @Override
   public CompletableFuture<Void> createTemplate(String name, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      Cache<ScopedState, Object> cache = getStateCache();
      ScopedState key = new ScopedState(TEMPLATE_SCOPE, name);
      return cache.containsKeyAsync(key).thenCompose(exists -> {
         if (exists)
            throw CONFIG.configAlreadyDefined(name);
         return cache.putAsync(key, new CacheState(null, parserRegistry.serialize(name, configuration), flags));
      }).thenApply(v -> null);
   }

   @Override
   public CompletableFuture<Configuration> getOrCreateTemplate(String name, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      localConfigurationManager.validateFlags(flags);
      try {
         CacheState state = new CacheState(null, parserRegistry.serialize(name, configuration), flags);
         return getStateCache().putIfAbsentAsync(new ScopedState(TEMPLATE_SCOPE, name), state).thenApply((v) -> configuration);
      } catch (Exception e) {
         throw CONFIG.configurationSerializationFailed(name, configuration, e);
      }
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

   CompletableFuture<Void> createTemplateLocally(String name, CacheState state) {
      log.debugf("Starting template %s from global state", name);
      CompletionStage<Configuration> configurationStage = buildConfiguration(name, state, true);
      return configurationStage.thenCompose(configuration -> localConfigurationManager.createTemplate(name, configuration, state.getFlags()))
            .toCompletableFuture();
   }

   CompletableFuture<Void> createCacheLocally(String name, CacheState state) {
      log.debugf("Starting cache %s from global state", name);
      CompletionStage<Configuration> configurationStage = buildConfiguration(name, state, false);
      return configurationStage.thenCompose(configuration -> localConfigurationManager.createCache(name, state.getTemplate(), configuration, state.getFlags()))
            .toCompletableFuture();
   }

   private CompletionStage<Configuration> buildConfiguration(String name, CacheState state, boolean template) {
      ConfigurationBuilderHolder builderHolder = parserRegistry.parse(state.getConfiguration());
      Configuration config = builderHolder.getNamedConfigurationBuilders().get(name).template(template).build(configurationManager.getGlobalConfiguration());
      return CompletableFuture.completedFuture(config);
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

   @Override
   public CompletableFuture<Void> removeTemplate(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return getStateCache().removeAsync(new ScopedState(TEMPLATE_SCOPE, name)).thenCompose((r) -> CompletableFutures.completedNull());
   }

   CompletableFuture<Void> removeCacheLocally(String name, CacheState state) {
      return localConfigurationManager.removeCache(name, state.getFlags());
   }

   CompletableFuture<Void> removeTemplateLocally(String name, CacheState state) {
      return localConfigurationManager.removeTemplate(name, state.getFlags());
   }
}
