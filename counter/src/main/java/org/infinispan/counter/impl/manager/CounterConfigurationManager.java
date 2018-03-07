package org.infinispan.counter.impl.manager;

import static org.infinispan.counter.configuration.ConvertUtil.parsedConfigToConfig;
import static org.infinispan.counter.util.Utils.validateStrongCounterBounds;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.configuration.AbstractCounterConfiguration;
import org.infinispan.counter.configuration.CounterManagerConfiguration;
import org.infinispan.counter.impl.CounterModuleLifecycle;
import org.infinispan.counter.logging.Log;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.globalstate.ScopeFilter;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.stream.CacheCollectors;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.LogFactory;

/**
 * Stores all the defined counter's configuration.
 * <p>
 * It uses the state-{@link Cache} to distribute the counter's configuration among the nodes in the cluster, and the {@link CounterConfigurationStorage} to persist persistent configurations.
 * <p>
 * Note that the {@link Cache} doesn't persist anything.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CounterConfigurationManager {

   public static final String COUNTER_SCOPE = "counter";
   private static final Log log = LogFactory.getLog(CounterConfigurationManager.class, Log.class);
   private final AtomicBoolean counterCacheStarted = new AtomicBoolean(false);
   private final EmbeddedCacheManager cacheManager;
   private final List<AbstractCounterConfiguration> configuredCounters;
   private final CounterConfigurationStorage storage;


   private volatile AdvancedCache<ScopedState, CounterConfiguration> stateCache;
   private volatile CounterConfigurationListener listener;

   CounterConfigurationManager(EmbeddedCacheManager cacheManager, CounterConfigurationStorage storage) {
      this.cacheManager = cacheManager;
      this.storage = storage;
      GlobalConfiguration globalConfig = cacheManager.getGlobalComponentRegistry().getGlobalConfiguration();
      CounterManagerConfiguration counterManagerConfig = globalConfig.module(CounterManagerConfiguration.class);
      this.configuredCounters = counterManagerConfig == null ?
            Collections.emptyList() :
            counterManagerConfig.counters();
   }


   private static ScopedState stateKey(String counterName) {
      return new ScopedState(COUNTER_SCOPE, counterName);
   }

   /**
    * It checks for existing defined configurations in {@link CounterConfigurationStorage} and in the {@link Cache}.
    * <p>
    * If any is found, it starts the counter's {@link Cache}.
    */
   public void start() {
      this.stateCache = cacheManager
            .<ScopedState, CounterConfiguration>getCache(GlobalConfigurationManager.CONFIG_STATE_CACHE_NAME)
            .getAdvancedCache();
      listener = new CounterConfigurationListener();
      Map<String, CounterConfiguration> persisted = storage.loadAll();
      persisted.forEach((name, cfg) -> stateCache.putIfAbsent(stateKey(name), cfg));
      counterCacheStarted.set(false);
      stateCache.addListener(listener, new ScopeFilter(COUNTER_SCOPE));
      if (!persisted.isEmpty() || stateCache.keySet().stream()
            .anyMatch(scopedState -> COUNTER_SCOPE.equals(scopedState.getScope()))) {
         //we have counter defined
         startCounterCache();
      }
   }


   /**
    * Removes the listener for new coming defined counter's.
    * <p>
    * The persistence is done on the fly when the configuration is defined.
    */
   public void stop() {
      counterCacheStarted.set(true); //avoid starting the counter cache if it hasn't started yet
      if (listener != null && stateCache != null) {
         stateCache.removeListener(listener);
      }
      listener = null;
      stateCache = null;
   }

   /**
    * It defines a new counter with the {@link CounterConfiguration}.
    *
    * @param name          the counter's name.
    * @param configuration the counter's {@link CounterConfiguration}.
    * @return {@code true} if the counter doesn't exist. {@code false} otherwise.
    */
   public CompletableFuture<Boolean> defineConfiguration(String name, CounterConfiguration configuration) {
      //first, check if valid.
      validateConfiguration(configuration);
      return checkGlobalConfiguration(name)
            .thenCompose(fileConfig -> {
               if (fileConfig != null) {
                  //counter configuration exists in configuration file!
                  return CompletableFuture.completedFuture(Boolean.FALSE);
               }
               //put configuration in cache
               return stateCache.putIfAbsentAsync(stateKey(name), configuration)
                     .thenApply(cfg -> {
                        if (cfg == null) {
                           //the counter was created. we need to persist it (if the counter is persistent)
                           storage.store(name, configuration);
                           return Boolean.TRUE;
                        } else {
                           //already defined.
                           return Boolean.FALSE;
                        }
                     });
            });
   }

   /**
    * @return all the defined counter's name, even the one in Infinispan's configuration.
    */
   public Collection<String> getCounterNames() {
      Collection<String> countersName = stateCache.keySet().stream()
            .filter(new ScopeFilter(COUNTER_SCOPE))
            .map(ScopedState::getName)
            .collect(CacheCollectors.serializableCollector(Collectors::toSet));
      configuredCounters.stream().map(AbstractCounterConfiguration::name).forEach(countersName::add);
      return Collections.unmodifiableCollection(countersName);
   }

   /**
    * Returns the counter's configuration.
    *
    * @param name the counter's name.
    * @return the counter's {@link CounterConfiguration} or {@code null} if not defined.
    */
   CompletableFuture<CounterConfiguration> getConfiguration(String name) {
      return stateCache.getAsync(stateKey(name)).thenCompose(existingConfiguration -> {
         if (existingConfiguration == null) {
            return checkGlobalConfiguration(name);
         } else {
            return CompletableFuture.completedFuture(existingConfiguration);
         }
      });
   }

   /**
    * Checks if the counter is defined in the Infinispan's configuration file.
    *
    * @param name the counter's name.
    * @return {@code null} if the counter isn't defined yet. {@link CounterConfiguration} if it is defined.
    */
   private CompletableFuture<CounterConfiguration> checkGlobalConfiguration(String name) {
      for (AbstractCounterConfiguration config : configuredCounters) {
         if (config.name().equals(name)) {
            CounterConfiguration cConfig = parsedConfigToConfig(config);
            return stateCache.putIfAbsentAsync(stateKey(name), cConfig)
                  .thenApply(configuration -> {
                     if (configuration == null) {
                        storage.store(name, cConfig);
                        return cConfig;
                     }
                     return configuration;
                  });
         }
      }
      return CompletableFutures.completedNull();
   }

   /**
    * Invoked when a new configuration is stored in the state-cache, it boots the counter's cache.
    */
   private void createCounter() {
      if (stateCache.getStatus() == ComponentStatus.RUNNING) {
         startCounterCache();
      }
   }

   /**
    * Validates the counter's configuration.
    *
    * @param configuration the {@link CounterConfiguration} to be validated.
    */
   private void validateConfiguration(CounterConfiguration configuration) {
      storage.validatePersistence(configuration);
      switch (configuration.type()) {
         case BOUNDED_STRONG:
            validateStrongCounterBounds(configuration.lowerBound(), configuration.initialValue(),
                  configuration.upperBound());
            break;
         case WEAK:
            if (configuration.concurrencyLevel() < 1) {
               throw log.invalidConcurrencyLevel(configuration.concurrencyLevel());
            }
            break;
      }
   }

   private void startCounterCache() {
      if (counterCacheStarted.compareAndSet(false, true)) {
         cacheManager.getGlobalComponentRegistry()
               .getComponent(Executor.class, KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR)
               .execute(() -> {
                  String oldName = Thread.currentThread().getName();
                  try {
                     Thread.currentThread().setName(threadName());
                     cacheManager.getCache(CounterModuleLifecycle.COUNTER_CACHE_NAME);
                  } finally {
                     Thread.currentThread().setName(oldName);
                  }
               });
      }
   }

   private String threadName() {
      GlobalConfiguration configuration = cacheManager.getCacheManagerConfiguration();
      return "CounterCacheStartThread," + configuration.transport().nodeName();
   }

   //can be async!
   @Listener(observation = Listener.Observation.POST, sync = false)
   private class CounterConfigurationListener {

      @CacheEntryModified
      @CacheEntryCreated
      public void onNewCounterConfiguration(Event<ScopedState, CounterConfiguration> event) {
         createCounter();
      }

   }
}
