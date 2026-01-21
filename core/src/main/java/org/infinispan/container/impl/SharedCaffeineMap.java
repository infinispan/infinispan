package org.infinispan.container.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.container.entries.CacheEntrySizeCalculator;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.core.JOLEntrySizeCalculator;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.openjdk.jol.info.ClassLayout;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

@Scope(Scopes.GLOBAL)
public class SharedCaffeineMap<K, V> {
   private final Cache<KeyValuePair<String, K>, InternalCacheEntry<K, V>> cache;
   private final ConcurrentMap<String, EvictionListener<K, V>> listenerMap;

   private static final long KEY_VALUE_PAIR_SIZE = ClassLayout.parseInstance(new KeyValuePair<>(null, null)).instanceSize();

   static <K, V> Caffeine<K, V> caffeineBuilder() {
      //noinspection unchecked
      return (Caffeine<K, V>) Caffeine.newBuilder();
   }

   public Cache<KeyValuePair<String, K>, InternalCacheEntry<K, V>> getCache() {
      return cache;
   }

   public ConcurrentMap<String, EvictionListener<K, V>> getListenerMap() {
      return listenerMap;
   }

   public SharedCaffeineMap(long thresholdSize, boolean memoryBased) {
      Caffeine<KeyValuePair<String, K>, InternalCacheEntry<K, V>> caffeine = caffeineBuilder();

      if (memoryBased) {
         CacheEntrySizeCalculator<K, V> calc = new CacheEntrySizeCalculator<>(JOLEntrySizeCalculator.getInstance());
         // Note the cache name is a reference so it only costs the object reference and not the actual String contents.
         caffeine.weigher((k, v) ->
               (int) KEY_VALUE_PAIR_SIZE + (int) calc.calculateSize(k.getValue(), v))
               .maximumWeight(thresholdSize);
      } else {
         caffeine.maximumSize(thresholdSize);
      }
      caffeine.executor(new WithinThreadExecutor()).evictionListener((key, value, cause) -> {
         if (cause == RemovalCause.SIZE) {
            notifyListener(true, key, value);
         }
      }).removalListener((key, value, cause) -> {
         if (cause == RemovalCause.SIZE) {
            notifyListener(false, key, value);
         }
      });
      cache = caffeine.build();
      listenerMap = new ConcurrentHashMap<>();
   }

   private void notifyListener(boolean pre, KeyValuePair<String, K> kvp, InternalCacheEntry<K, V> ice) {
      EvictionListener<K, V> listener = listenerMap.get(kvp.getKey());
      if (listener != null) {
         listener.onEntryChosenForEviction(pre, kvp.getValue(), ice);
      }
   }

   public SharedBoundedContainer<K, V> newContainer(String cacheName, BasicComponentRegistry componentRegistry,
                                                    int numSegments) {
      PeekableTouchableCaffeineMap<K, V> ptcm = new PeekableTouchableCaffeineMap<>(new CaffeineCacheMapper<>(cache, KeyValuePair::getValue,
            k -> new KeyValuePair<>(cacheName, k)));
      SharedBoundedContainer<K, V> container = new SharedBoundedContainer<>(ptcm, PeekableTouchableContainerMap::new, numSegments);
      if (listenerMap.putIfAbsent(cacheName, container) != null) {
         throw new IllegalArgumentException("Container already exists for cache: " + cacheName);
      }
      componentRegistry.wireDependencies(container, false);
      container.start();
      return container;
   }
}
