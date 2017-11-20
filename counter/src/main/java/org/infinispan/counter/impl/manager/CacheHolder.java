package org.infinispan.counter.impl.manager;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.configuration.AbstractCounterConfiguration;
import org.infinispan.counter.configuration.StrongCounterConfiguration;
import org.infinispan.counter.configuration.WeakCounterConfiguration;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.listener.CounterManagerNotificationManager;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * It holds the caches used by {@link EmbeddedCounterManager}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class CacheHolder {
   private final AdvancedCache<String, CounterConfiguration> configurationCache;
   private final AdvancedCache<? extends CounterKey, CounterValue> counterCache;
   private final List<AbstractCounterConfiguration> defaultCounters;

   public CacheHolder(AdvancedCache<String, CounterConfiguration> configurationCache,
         AdvancedCache<? extends CounterKey, CounterValue> counterCache,
         List<AbstractCounterConfiguration> defaultCounters) {
      this.configurationCache = configurationCache;
      this.counterCache = counterCache;
      this.defaultCounters = defaultCounters;
   }

   Collection<String> getCounterNames() {
      Set<String> countersName = new HashSet<>(configurationCache.keySet());
      defaultCounters.stream().map(AbstractCounterConfiguration::name).forEach(countersName::add);
      return Collections.unmodifiableCollection(countersName);
   }

   /**
    * Registers the {@link CounterManagerNotificationManager}'s listeners to the cache hold by this instance.
    * <p>
    * It invokes {@link CounterManagerNotificationManager#listenOn(Cache)} method and it should protect itself to avoid
    * registering multiple times.
    *
    * @param notificationManager the {@link CounterManagerNotificationManager} with the listeners to register
    */
   void registerNotificationManager(CounterManagerNotificationManager notificationManager) {
      notificationManager.listenOn(counterCache);
   }

   CompletableFuture<Boolean> addConfigurationAsync(String name, CounterConfiguration configuration) {
      return checkAndStoreConfiguredCounterConfigurationAsync(name)
            .thenCompose(fileConfig -> {
               if (fileConfig != null) {
                  //counter configuration exists in configuration file!
                  return CompletableFuture.completedFuture(fileConfig);
               }
               //put configuration in cache
               return configCacheWithFlags(configuration).putIfAbsentAsync(name, configuration);
            }).thenApply(Objects::isNull);
   }

   <K extends CounterKey> AdvancedCache<K, CounterValue> getCounterCache(CounterConfiguration configuration) {
      //noinspection unchecked
      return (AdvancedCache<K, CounterValue>) (configuration.storage() == Storage.VOLATILE ?
            counterCache.withFlags(Flag.SKIP_CACHE_LOAD, Flag.SKIP_CACHE_STORE) :
            counterCache);
   }

   CompletableFuture<CounterConfiguration> getConfigurationAsync(String counterName) {
      return configurationCache.getAsync(counterName).thenCompose(existingConfiguration -> {
         if (existingConfiguration == null) {
            return checkAndStoreConfiguredCounterConfigurationAsync(counterName);
         } else {
            return CompletableFuture.completedFuture(existingConfiguration);
         }
      });
   }

   private Cache<String, CounterConfiguration> configCacheWithFlags(CounterConfiguration config) {
      return config.storage() == Storage.VOLATILE ?
             configurationCache.withFlags(Flag.SKIP_CACHE_STORE, Flag.SKIP_CACHE_LOAD) :
             configurationCache;
   }

   private CompletableFuture<CounterConfiguration> checkAndStoreConfiguredCounterConfigurationAsync(String name) {
      for (AbstractCounterConfiguration config : defaultCounters) {
         if (config.name().equals(name)) {
            CounterConfiguration counterConfiguration = createConfigurationEntry(config);
            return configurationCache.putIfAbsentAsync(name, counterConfiguration)
                  .thenApply(configuration -> configuration == null ? counterConfiguration : configuration);
         }
      }
      return CompletableFutures.completedNull();
   }

   private static CounterConfiguration createConfigurationEntry(AbstractCounterConfiguration configuration) {
      if (configuration instanceof StrongCounterConfiguration) {
         if (((StrongCounterConfiguration) configuration).isBound()) {
            return CounterConfiguration.builder(CounterType.BOUNDED_STRONG).initialValue(configuration.initialValue())
                  .lowerBound(((StrongCounterConfiguration) configuration).lowerBound())
                  .upperBound(((StrongCounterConfiguration) configuration).upperBound())
                  .storage(configuration.storage()).build();
         } else {
            return CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).initialValue(configuration.initialValue())
                  .storage(configuration.storage()).build();
         }
      } else if (configuration instanceof WeakCounterConfiguration) {
         return CounterConfiguration.builder(CounterType.WEAK).initialValue(configuration.initialValue())
               .storage(configuration.storage())
               .concurrencyLevel(((WeakCounterConfiguration) configuration).concurrencyLevel()).build();
      }
      throw new IllegalStateException(
            "[should never happen] unknown CounterConfiguration class: " + configuration.getClass());
   }
}
