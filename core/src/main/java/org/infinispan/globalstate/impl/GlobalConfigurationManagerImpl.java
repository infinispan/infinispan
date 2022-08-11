package org.infinispan.globalstate.impl;

import static org.infinispan.util.logging.Log.CONFIG;

import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.util.concurrent.CompletableFutures;
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
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.event.ConfigurationChangedEvent;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.BlockingManager;
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

   @Inject
   EmbeddedCacheManager cacheManager;
   @Inject
   LocalTopologyManager localTopologyManager;
   @Inject
   ConfigurationManager configurationManager;
   @Inject
   InternalCacheRegistry internalCacheRegistry;
   @Inject
   GlobalComponentRegistry globalComponentRegistry;
   @Inject
   BlockingManager blockingManager;
   @Inject
   CacheManagerNotifier cacheManagerNotifier;

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

      Set<String> internalCacheNames = new TreeSet<>(internalCacheRegistry.getInternalCacheNames());
      log.debugf("Starting internal caches: %s", internalCacheNames);
      for (String cacheName : internalCacheNames) {
         SecurityActions.getCache(cacheManager, cacheName);
      }

      Set<String> staticCacheNames = new TreeSet<>(configurationManager.getDefinedCaches());
      staticCacheNames.removeAll(internalCacheNames);
      log.debugf("Starting user defined caches: %s", staticCacheNames);
      for (String cacheName : staticCacheNames) {
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
            CompletionStage<Void> future = cacheScope ? createCacheLocally(name, state) : createTemplateLocally(name, state);
            CompletionStages.join(future);
         }
      });

      EnumSet<CacheContainerAdmin.AdminFlag> adminFlags = EnumSet.noneOf(CacheContainerAdmin.AdminFlag.class);
      persistedCaches.forEach((name, configuration) -> {
         ensurePersistenceCompatibility(name, configuration);
         // First create the cache locally to ensure that it's available on startup
         createCacheLocally(name, null, configuration, adminFlags);
         // The cache configuration was permanent, it still needs to be
         CompletionStages.join(getOrCreateCache(name, configuration, adminFlags));
      });

      persistedTemplates.forEach((name, configuration) -> {
         ensurePersistenceCompatibility(name, configuration);
         // First create the cache locally to ensure that it's available on startup
         createTemplateLocally(name, configuration, adminFlags);
         // The template was permanent, it still needs to be
         CompletionStages.join(getOrCreateTemplate(name, configuration, adminFlags));
      });
   }

   private void ensureClusterCompatibility(String name, CacheState state, Map<String, Configuration> configs) {
      Configuration persisted = configs.get(name);
      if (persisted != null) {
         // Template value is not serialized, so buildConfiguration param is irrelevant
         Configuration configuration = buildConfiguration(name, state.getConfiguration(), false);
         if (!persisted.matches(configuration))
            throw CONFIG.incompatibleClusterConfiguration(name, configuration, persisted);
      }
   }

   private void ensurePersistenceCompatibility(String name, Configuration configuration) {
      Configuration staticConfiguration = cacheManager.getCacheConfiguration(name);
      if (staticConfiguration != null && !staticConfiguration.matches(configuration))
         throw CONFIG.incompatiblePersistedConfiguration(name, configuration, staticConfiguration);
   }

   private void assertNameLength(String name) {
      if (!ByteString.isValid(name)) {
         throw CONFIG.invalidNameSize(name);
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
   public CompletionStage<Void> createTemplate(String name, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      assertNameLength(name);

      Cache<ScopedState, Object> cache = getStateCache();
      ScopedState key = new ScopedState(TEMPLATE_SCOPE, name);
      return cache.containsKeyAsync(key).thenCompose(exists -> {
         if (exists)
            throw CONFIG.configAlreadyDefined(name);
         return cache.putAsync(key, new CacheState(null, parserRegistry.serialize(name, configuration), flags));
      }).thenApply(v -> null);
   }

   @Override
   public CompletionStage<Configuration> getOrCreateTemplate(String name, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      assertNameLength(name);
      localConfigurationManager.validateFlags(flags);
      try {
         final CacheState state = new CacheState(null, parserRegistry.serialize(name, configuration), flags);
         return getStateCache().putIfAbsentAsync(new ScopedState(TEMPLATE_SCOPE, name), state).thenApply((v) -> configuration);
      } catch (Exception e) {
         throw CONFIG.configurationSerializationFailed(name, configuration, e);
      }
   }

   @Override
   public CompletionStage<Configuration> createCache(String cacheName, Configuration configuration,
                                                     EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      if (cacheManager.cacheExists(cacheName)) {
         throw CONFIG.cacheExists(cacheName);
      } else {
         return getOrCreateCache(cacheName, configuration, flags);
      }
   }

   @Override
   public CompletionStage<Configuration> getOrCreateCache(String cacheName, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return createCache(cacheName, null, configuration, flags);
   }

   @Override
   public CompletionStage<Configuration> createCache(String cacheName, String template, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      if (cacheManager.cacheExists(cacheName)) {
         throw CONFIG.cacheExists(cacheName);
      } else {
         return getOrCreateCache(cacheName, template, flags);
      }
   }

   @Override
   public CompletionStage<Configuration> getOrCreateCache(String cacheName, String template, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
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

   CompletionStage<Configuration> createCache(String cacheName, String template, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      assertNameLength(cacheName);

      localConfigurationManager.validateFlags(flags);
      final CacheState state;
      try {
         state = new CacheState(template, parserRegistry.serialize(cacheName, configuration), flags);
      } catch (Exception e) {
         throw CONFIG.configurationSerializationFailed(cacheName, configuration, e);
      }
      if (flags.contains(CacheContainerAdmin.AdminFlag.UPDATE)) {
         if (internalCacheRegistry.isInternalCache(cacheName)) {
            throw CONFIG.cannotUpdateInternalCache(cacheName);
         }
         return getStateCache().putAsync(new ScopedState(CACHE_SCOPE, cacheName), state)
               .thenApply((v) -> configuration);
      } else {
         return getStateCache().putIfAbsentAsync(new ScopedState(CACHE_SCOPE, cacheName), state)
               .thenApply((v) -> configuration);
      }
   }

   CompletionStage<Void> createTemplateLocally(String name, CacheState state) {
      Configuration configuration = buildConfiguration(name, state.getConfiguration(), true);
      return createTemplateLocally(name, configuration, state.getFlags());
   }

   CompletionStage<Void> createTemplateLocally(String name, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      log.debugf("Creating template %s from global state", name);
      return localConfigurationManager.createTemplate(name, configuration, flags)
            .thenCompose(v -> cacheManagerNotifier.notifyConfigurationChanged(ConfigurationChangedEvent.EventType.CREATE, "template", name))
            .toCompletableFuture();
   }

   CompletionStage<Void> createCacheLocally(String name, CacheState state) {
      Configuration configuration = buildConfiguration(name, state.getConfiguration(), false);
      return createCacheLocally(name, state.getTemplate(), configuration, state.getFlags());
   }

   CompletionStage<Void> createCacheLocally(String name, String template, Configuration configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      log.debugf("Creating cache %s from global state", name);
      return localConfigurationManager.createCache(name, template, configuration, flags)
            .thenCompose(v -> cacheManagerNotifier.notifyConfigurationChanged(ConfigurationChangedEvent.EventType.CREATE, "cache", name))
            .toCompletableFuture();
   }

   CompletionStage<Void> validateConfigurationUpdateLocally(String name, CacheState state) {
      log.debugf("Validating configuration %s from global state", name);
      Configuration configuration = buildConfiguration(name, state.getConfiguration(), false);
      return localConfigurationManager.validateConfigurationUpdate(name, configuration, state.getFlags());
   }

   CompletionStage<Void> updateConfigurationLocally(String name, CacheState state) {
      log.debugf("Updating configuration %s from global state", name);
      Configuration configuration = buildConfiguration(name, state.getConfiguration(), false);
      return localConfigurationManager.updateConfiguration(name, configuration, state.getFlags())
            .thenCompose(v -> cacheManagerNotifier.notifyConfigurationChanged(ConfigurationChangedEvent.EventType.UPDATE, "cache", name));
   }

   private Configuration buildConfiguration(String name, String configStr, boolean template) {
      ConfigurationBuilderHolder builderHolder = parserRegistry.parse(configStr);
      return builderHolder.getNamedConfigurationBuilders().get(name).template(template).build(configurationManager.getGlobalConfiguration());
   }

   @Override
   public CompletionStage<Void> removeCache(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      ScopedState cacheScopedState = new ScopedState(CACHE_SCOPE, name);
      if (getStateCache().containsKey(cacheScopedState)) {
         try {
            localTopologyManager.setCacheRebalancingEnabled(name, false);
         } catch (Exception e) {
            // Ignore
         }
         return getStateCache().removeAsync(cacheScopedState).thenCompose(r -> CompletableFutures.completedNull());
      } else {
         return localConfigurationManager.removeCache(name, flags);
      }
   }

   @Override
   public CompletionStage<Void> removeTemplate(String name, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      return getStateCache().removeAsync(new ScopedState(TEMPLATE_SCOPE, name)).thenCompose((r) -> CompletableFutures.completedNull());
   }

   CompletionStage<Void> removeCacheLocally(String name) {
      return localConfigurationManager.removeCache(name, EnumSet.noneOf(CacheContainerAdmin.AdminFlag.class)).thenCompose(v -> cacheManagerNotifier.notifyConfigurationChanged(ConfigurationChangedEvent.EventType.REMOVE, "cache", name));
   }

   CompletionStage<Void> removeTemplateLocally(String name) {
      return localConfigurationManager.removeTemplate(name, EnumSet.noneOf(CacheContainerAdmin.AdminFlag.class)).thenCompose(v -> cacheManagerNotifier.notifyConfigurationChanged(ConfigurationChangedEvent.EventType.REMOVE, "template", name));
   }
}
