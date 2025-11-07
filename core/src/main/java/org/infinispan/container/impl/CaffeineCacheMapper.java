package org.infinispan.container.impl;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.infinispan.commons.util.InjectiveFunction;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.commons.util.KeyMapperMap;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

/**
 * Mapper class that can be used with caffeine based caches to map a key to a different key as necessary. Note this class
 * does not implement mapping for all optional classes provided from caffeine and is thus why it is in an impl package
 * of core and not in the commons module like {@link KeyMapperMap} which does.
 * @param delegate The actual Caffeine cache to map the key to and from
 * @param keyMapper the way to map the key to a different type
 * @param keyUnmapper the way to unmap the different type to the cache key type
 * @param <K> The original cache key type
 * @param <R> The mapped type
 * @param <V> The value type of the Caffeine cache which is unchanged
 */
public record CaffeineCacheMapper<K, R, V>(Cache<K, V> delegate, InjectiveFunction<? super K, ? extends R> keyMapper,
                                           InjectiveFunction<? super R, ? extends K> keyUnmapper) implements Cache<R, V> {

   @Override
   public @Nullable V getIfPresent(Object key) {
      return delegate.getIfPresent(keyUnmapper.apply((R) key));
   }

   @Override
   public V get(R key, Function<? super R, ? extends V> mappingFunction) {
      return delegate.get(keyUnmapper.apply(key), k -> mappingFunction.apply(key));
   }

   @Override
   public Map<R, @NonNull V> getAllPresent(Iterable<? extends R> keys) {
      return getAll(keys, s -> Map.of());
   }

   @Override
   public Map<R, @NonNull V> getAll(Iterable<? extends R> keys, Function<? super Set<? extends R>, ? extends Map<? extends R, ? extends @NonNull V>> mappingFunction) {
      return delegate.getAll(StreamSupport.stream(keys.spliterator(), false).map(keyUnmapper).collect(Collectors.toSet()),
                  key -> {
                     Set<R> rKeys = key.stream().map(keyMapper).collect(Collectors.toSet());
                     Map<? extends R, ? extends V> rMap = mappingFunction.apply(rKeys);
                     return rMap.entrySet().stream()
                           .collect(Collectors.toMap(entry -> keyUnmapper.apply(entry.getKey()), Map.Entry::getValue));
                  }).entrySet().stream()
            .collect(Collectors.toMap(entry -> keyMapper.apply(entry.getKey()), Map.Entry::getValue));
   }

   @Override
   public void put(R key, @NonNull V value) {
      delegate.put(keyUnmapper.apply(key), value);
   }

   @Override
   public void putAll(Map<? extends R, ? extends @NonNull V> map) {
      delegate.putAll(map.entrySet().stream()
            .collect(Collectors.toMap(entry -> keyUnmapper.apply(entry.getKey()), Map.Entry::getValue)));
   }

   @Override
   public void invalidateAll(Iterable<? extends R> keys) {
      delegate.invalidateAll(() -> new IteratorMapper<>(keys.iterator(), keyUnmapper));
   }

   @Override
   public void invalidate(Object key) {
      delegate.invalidate(keyUnmapper.apply((R) key));
   }

   @Override
   public void invalidateAll() {
      delegate.invalidateAll();
   }

   @Override
   public long estimatedSize() {
      return delegate.estimatedSize();
   }

   @Override
   public CacheStats stats() {
      return delegate.stats();
   }

   @Override
   public void cleanUp() {
      delegate.cleanUp();
   }

   @Override
   public Policy<R, @NonNull V> policy() {
      return new PolicyMapper<>(delegate.policy(), keyMapper, keyUnmapper);
   }

   private record PolicyMapper<K, R, V>(Policy<K, V> delegatePolicy, Function<? super K, ? extends R> keyMapper,
                                        Function<? super R, ? extends K> keyUnmapper) implements Policy<R, V> {
      @Override
      public @Nullable V getIfPresentQuietly(Object key) {
         return delegatePolicy.getIfPresentQuietly(keyUnmapper.apply((R) key));
      }

      @Override
      public boolean isRecordingStats() {
         return delegatePolicy.isRecordingStats();
      }

      @Override
      public Optional<Eviction<R, V>> eviction() {
         // This only will support non key methods.. this is okay for now
         return (Optional) delegatePolicy.eviction();
      }

      @Override
      public Map<R, CompletableFuture<V>> refreshes() {
         throw new UnsupportedOperationException();
      }

      @Override
      public Optional<FixedExpiration<R, V>> expireAfterAccess() {
         throw new UnsupportedOperationException();
      }

      @Override
      public Optional<FixedExpiration<R, V>> expireAfterWrite() {
         throw new UnsupportedOperationException();
      }

      @Override
      public Optional<VarExpiration<R, V>> expireVariably() {
         throw new UnsupportedOperationException();
      }

      @Override
      public Optional<FixedRefresh<R, V>> refreshAfterWrite() {
         throw new UnsupportedOperationException();
      }
   }

   @Override
   public ConcurrentMap<R, @NonNull V> asMap() {
      return new KeyMapperMap<>(delegate.asMap(), keyMapper, keyUnmapper);
   }
}
