package org.infinispan.lock.impl;

import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.lock.impl.entries.ClusteredLockKey;
import org.infinispan.lock.impl.entries.ClusteredLockValue;
import org.infinispan.lock.impl.functions.IsLocked;
import org.infinispan.lock.impl.functions.LockFunction;
import org.infinispan.lock.impl.functions.UnlockFunction;
import org.infinispan.lock.impl.lock.ClusteredLockFilter;
import org.infinispan.lock.impl.manager.CacheHolder;
import org.infinispan.lock.impl.manager.EmbeddedClusteredLockManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.transaction.TransactionMode;
import org.kohsuke.MetaInfServices;

/**
 * Locks module configuration
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@MetaInfServices(value = ModuleLifecycle.class)
public class ClusteredLockModuleLifecycle implements ModuleLifecycle {

   public static final String CLUSTERED_LOCK_CACHE_NAME = "org.infinispan.LOCKS";

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      final Map<Integer, AdvancedExternalizer<?>> externalizerMap = globalConfiguration.serialization()
            .advancedExternalizers();
      externalizerMap.put(ClusteredLockKey.EXTERNALIZER.getId(), ClusteredLockKey.EXTERNALIZER);
      externalizerMap.put(ClusteredLockValue.EXTERNALIZER.getId(), ClusteredLockValue.EXTERNALIZER);
      externalizerMap.put(LockFunction.EXTERNALIZER.getId(), LockFunction.EXTERNALIZER);
      externalizerMap.put(UnlockFunction.EXTERNALIZER.getId(), UnlockFunction.EXTERNALIZER);
      externalizerMap.put(IsLocked.EXTERNALIZER.getId(), IsLocked.EXTERNALIZER);
      externalizerMap.put(ClusteredLockFilter.EXTERNALIZER.getId(), ClusteredLockFilter.EXTERNALIZER);
   }

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      final EmbeddedCacheManager cacheManager = gcr.getComponent(EmbeddedCacheManager.class);
      final InternalCacheRegistry internalCacheRegistry = gcr.getComponent(InternalCacheRegistry.class);

      internalCacheRegistry.registerInternalCache(CLUSTERED_LOCK_CACHE_NAME, createClusteredLockCacheConfiguration(),
            EnumSet.of(InternalCacheRegistry.Flag.EXCLUSIVE));

      CompletableFuture<CacheHolder> future = startCaches(cacheManager);
      registerClusteredLockManager(gcr, future);
   }

   private static Configuration createClusteredLockCacheConfiguration() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.REPL_SYNC)
            .stateTransfer().fetchInMemoryState(true)
            .partitionHandling().whenSplit(PartitionHandling.DENY_READ_WRITES)
            .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);

      return builder.build();
   }

   private static CompletableFuture<CacheHolder> startCaches(EmbeddedCacheManager cacheManager) {
      final CompletableFuture<CacheHolder> future = new CompletableFuture<>();
      new Thread(() -> {
         try {
            Cache<? extends ClusteredLockKey, ClusteredLockValue> locksCache = cacheManager.getCache(CLUSTERED_LOCK_CACHE_NAME);
            future.complete(
                  new CacheHolder(locksCache.getAdvancedCache()));
         } catch (Throwable throwable) {
            future.completeExceptionally(throwable);
         }
      }).start();
      return future;
   }

   private static void registerClusteredLockManager(GlobalComponentRegistry registry, CompletableFuture<CacheHolder> future) {
      //noinspection SynchronizationOnLocalVariableOrMethodParameter
      synchronized (registry) {
         ClusteredLockManager counterManager = registry.getComponent(ClusteredLockManager.class);
         if (counterManager == null || !(counterManager instanceof EmbeddedClusteredLockManager)) {
            counterManager = new EmbeddedClusteredLockManager(future);
            registry.registerComponent(counterManager, ClusteredLockManager.class);
         }
      }
   }

}
