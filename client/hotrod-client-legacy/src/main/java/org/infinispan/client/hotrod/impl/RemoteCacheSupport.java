package org.infinispan.client.hotrod.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.client.hotrod.impl.Util.await;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;

/**
 * Purpose: keep all delegating and unsupported methods in one place -> readability.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class RemoteCacheSupport<K, V> implements RemoteCache<K, V> {
   protected long defaultLifespan;
   protected long defaultMaxIdleTime;

   protected RemoteCacheSupport() {
      this(0, 0);
   }

   protected RemoteCacheSupport(long defaultLifespan, long defaultMaxIdleTime) {
      this.defaultLifespan = defaultLifespan;
      this.defaultMaxIdleTime = defaultMaxIdleTime;
   }

   @Override
   public final void putAll(Map<? extends K, ? extends V> map) {
      putAll(map, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      putAll(map, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      await(putAllAsync(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public final CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return putAllAsync(data, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return putAllAsync(data, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public abstract CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan,
         TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   @Override
   public final V putIfAbsent(K key, V value) {
      return putIfAbsent(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      return putIfAbsent(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return await(putIfAbsentAsync(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public final CompletableFuture<V> putIfAbsentAsync(K key, V value) {
      return putIfAbsentAsync(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit) {
      return putIfAbsentAsync(key, value, lifespan, lifespanUnit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public abstract CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit,
         long maxIdle, TimeUnit maxIdleUnit);

   @Override
   public final boolean replace(K key, V oldValue, V newValue) {
      return replace(key, oldValue, newValue, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      return replace(key, oldValue, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return await(replaceAsync(key, oldValue, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public final CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return replaceAsync(key, oldValue, newValue, defaultLifespan, MILLISECONDS);
   }

   @Override
   public final CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return replaceAsync(key, oldValue, newValue, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public abstract CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit,
         long maxIdle, TimeUnit maxIdleUnit);

   @Override
   public final V replace(K key, V value) {
      return replace(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final V replace(K key, V value, long lifespan, TimeUnit unit) {
      return replace(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return await(replaceAsync(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public final CompletableFuture<V> replaceAsync(K key, V value) {
      return replaceAsync(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return replaceAsync(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public abstract CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle,
         TimeUnit maxIdleUnit);

   @Override
   public final V get(Object key) {
      return await(getAsync((K) key));
   }

   @Override
   public abstract CompletableFuture<V> getAsync(K key);

   @Override
   public final Map<K, V> getAll(Set<? extends K> keys) {
      return await(getAllAsync(keys));
   }

   @Override
   public abstract CompletableFuture<Map<K, V>> getAllAsync(Set<?> keys);

   @Override
   public final MetadataValue<V> getWithMetadata(K key) {
      return await(getWithMetadataAsync(key));
   }

   @Override
   public abstract CompletableFuture<MetadataValue<V>> getWithMetadataAsync(K key);

   @Override
   public final boolean containsKey(Object key) {
      return await(containsKeyAsync((K) key));
   }

   @Override
   public abstract CompletableFuture<Boolean> containsKeyAsync(K key);

   @Override
   public final V put(K key, V value) {
      return put(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final V put(K key, V value, long lifespan, TimeUnit unit) {
      return put(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return await(putAsync(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public final CompletableFuture<V> putAsync(K key, V value) {
      return putAsync(key, value, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return putAsync(key, value, lifespan, unit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public abstract CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle,
         TimeUnit maxIdleUnit);

   @Override
   public final boolean replaceWithVersion(K key, V newValue, long version) {
      return replaceWithVersion(key, newValue, version, 0);
   }

   @Override
   public final boolean replaceWithVersion(K key, V newValue, long version, int lifespanSeconds) {
      return replaceWithVersion(key, newValue, version, lifespanSeconds, 0);
   }

   @Override
   public final boolean replaceWithVersion(K key, V newValue, long version, int lifespanSeconds, int maxIdleTimeSeconds) {
      return replaceWithVersion(key, newValue, version, lifespanSeconds, SECONDS, maxIdleTimeSeconds, SECONDS);
   }

   @Override
   public final boolean replaceWithVersion(K key, V newValue, long version, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      return await(replaceWithVersionAsync(key, newValue, version, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit));
   }

   @Override
   public final CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version) {
      return replaceWithVersionAsync(key, newValue, version, 0);
   }

   @Override
   public final CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, int lifespanSeconds) {
      return replaceWithVersionAsync(key, newValue, version, lifespanSeconds, 0);
   }

   @Override
   public final CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version,
         int lifespanSeconds, int maxIdleSeconds) {
      return replaceWithVersionAsync(key, newValue, version, lifespanSeconds, SECONDS, maxIdleSeconds, SECONDS);
   }

   @Override
   public final V remove(Object key) {
      return await(removeAsync(key));
   }

   @Override
   public abstract CompletableFuture<V> removeAsync(Object key);

   @Override
   public final boolean remove(Object key, Object value) {
      return await(removeAsync(key, value));
   }

   @Override
   public abstract CompletableFuture<Boolean> removeAsync(Object key, Object value);

   @Override
   public final boolean removeWithVersion(K key, long version) {
      return await(removeWithVersionAsync(key, version));
   }

   @Override
   public abstract CompletableFuture<Boolean> removeWithVersionAsync(K key, long version);

   @Override
   public final V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return merge(key, value, remappingFunction, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan,
         TimeUnit lifespanUnit) {
      return merge(key, value, remappingFunction, lifespan, lifespanUnit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan,
                  TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return await(mergeAsync(key, value, remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public final CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return mergeAsync(key, value, remappingFunction, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return mergeAsync(key, value, remappingFunction, lifespan, lifespanUnit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public abstract CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit);

   @Override
   public final void clear() {
      await(clearAsync());
   }

   @Override
   public final V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return compute(key, remappingFunction, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return compute(key, remappingFunction, lifespan, lifespanUnit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return await(computeAsync(key, remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public final CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return computeAsync(key, remappingFunction, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return computeAsync(key, remappingFunction, lifespan, lifespanUnit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public abstract CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   @Override
   public final V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
      return computeIfAbsent(key, mappingFunction, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return computeIfAbsent(key, mappingFunction, lifespan, lifespanUnit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return await(computeIfAbsentAsync(key, mappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public final CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction) {
      return computeIfAbsentAsync(key, mappingFunction, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return computeIfAbsentAsync(key, mappingFunction, lifespan, lifespanUnit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public abstract CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   @Override
   public final V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return computeIfPresent(key, remappingFunction, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return computeIfPresent(key, remappingFunction, lifespan, lifespanUnit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return await(computeIfPresentAsync(key, remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public final CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return computeIfPresentAsync(key, remappingFunction, defaultLifespan, MILLISECONDS, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public final CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return computeIfPresentAsync(key, remappingFunction, lifespan, lifespanUnit, defaultMaxIdleTime, MILLISECONDS);
   }

   @Override
   public abstract CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   @Override
   public abstract void replaceAll(BiFunction<? super K, ? super V, ? extends V> function);

   @Override
   public final int size() {
      long size = await(sizeAsync());
      return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
   }

   @Override
   public abstract CompletableFuture<Long> sizeAsync();
}
