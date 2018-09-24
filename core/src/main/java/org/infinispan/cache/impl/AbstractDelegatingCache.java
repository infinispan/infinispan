package org.infinispan.cache.impl;

import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheSet;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.format.PropertyFormatter;
import org.infinispan.filter.KeyFilter;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;

/**
 * This is a convenient base class for implementing a cache delegate. The only constructor takes a {@link Cache}
 * argument, to which each method call is delegated. One can extend this class and override the method sub-set it is
 * interested in. There is also an similar implementation for {@link org.infinispan.AdvancedCache}: {@link
 * org.infinispan.cache.impl.AbstractDelegatingAdvancedCache}.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.cache.impl.AbstractDelegatingAdvancedCache
 */
public abstract class AbstractDelegatingCache<K, V> implements Cache<K, V> {

   private final Cache<K, V> cache;

   public AbstractDelegatingCache(Cache<K, V> cache) {
      this.cache = cache;
      if (cache == null) throw new IllegalArgumentException("Delegate cache cannot be null!");
   }

   @Override
   public void putForExternalRead(K key, V value) {
      cache.putForExternalRead(key, value);
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit unit) {
      cache.putForExternalRead(key, value, lifespan, unit);
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      cache.putForExternalRead(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public void evict(K key) {
      cache.evict(key);
   }

   @Override
   public org.infinispan.configuration.cache.Configuration getCacheConfiguration() {
      return cache.getCacheConfiguration();
   }

   @Override
   public boolean startBatch() {
      return cache.startBatch();
   }

   @Override
   public void endBatch(boolean successful) {
      cache.endBatch(successful);
   }

   @Override
   public String getName() {
      return cache.getName();
   }

   @ManagedAttribute(
         description = "Returns the cache name",
         displayName = "Cache name",
         dataType = DataType.TRAIT,
         displayType = DisplayType.SUMMARY
   )
   public String getCacheName() {
      String name = getName().equals(BasicCacheContainer.DEFAULT_CACHE_NAME) ? "Default Cache" : getName();
      return name + "(" + getCacheConfiguration().clustering().cacheMode().toString().toLowerCase() + ")";
   }

   @Override
   @ManagedAttribute(
         description = "Returns the version of Infinispan",
         displayName = "Infinispan version",
         dataType = DataType.TRAIT,
         displayType = DisplayType.SUMMARY
   )
   public String getVersion() {
      return cache.getVersion();
   }

   @Override
   public EmbeddedCacheManager getCacheManager() {
      return cache.getCacheManager();
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      return cache.put(key, value, lifespan, unit);
   }

   /**
    * Don't remove.
    * @see {@link org.infinispan.cache.impl.CacheSupport#set(Object, Object)}
    */
   protected void set(K key, V value) {
      cache.put(key, value);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      return cache.putIfAbsent(key, value, lifespan, unit);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      cache.putAll(map, lifespan, unit);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      return cache.replace(key, value, lifespan, unit);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      return cache.replace(key, oldValue, value, lifespan, unit);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.put(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.putIfAbsent(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      cache.putAll(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.replace(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.replace(key, oldValue, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
      cache.replaceAll(function);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value) {
      return cache.putAsync(key, value);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cache.putAsync(key, value, lifespan, unit);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.putAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return cache.putAllAsync(data);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return cache.putAllAsync(data, lifespan, unit);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.putAllAsync(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<Void> clearAsync() {
      return cache.clearAsync();
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value) {
      return cache.putIfAbsentAsync(key, value);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cache.putIfAbsentAsync(key, value, lifespan, unit);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.putIfAbsentAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> removeAsync(Object key) {
      return cache.removeAsync(key);
   }

   @Override
   public CompletableFuture<Boolean> removeAsync(Object key, Object value) {
      return cache.removeAsync(key, value);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value) {
      return cache.replaceAsync(key, value);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cache.replaceAsync(key, value, lifespan, unit);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.replaceAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return cache.replaceAsync(key, oldValue, newValue);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return cache.replaceAsync(key, oldValue, newValue, lifespan, unit);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.replaceAsync(key, oldValue, newValue, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return cache.computeAsync(key, remappingFunction);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return cache.computeAsync(key, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.computeAsync(key, remappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction) {
      return cache.computeIfAbsentAsync(key, mappingFunction);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return cache.computeIfAbsentAsync(key, mappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.computeIfAbsentAsync(key, mappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return cache.computeIfPresentAsync(key, remappingFunction);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return cache.computeIfPresentAsync(key, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.computeIfPresentAsync(key, remappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return cache.mergeAsync(key, value, remappingFunction);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return cache.mergeAsync(key, value, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.mergeAsync(key, value, remappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public AdvancedCache<K, V> getAdvancedCache() {
      return cache.getAdvancedCache();
   }

   @Override
   public ComponentStatus getStatus() {
      return cache.getStatus();
   }

   /**
    * Returns String representation of ComponentStatus enumeration in order to avoid class not found exceptions in JMX
    * tools that don't have access to infinispan classes.
    */
   @ManagedAttribute(
         description = "Returns the cache status",
         displayName = "Cache status",
         dataType = DataType.TRAIT,
         displayType = DisplayType.SUMMARY
   )
   public String getCacheStatus() {
      return getStatus().toString();
   }

   @Override
   public V putIfAbsent(K key, V value) {
      return cache.putIfAbsent(key, value);
   }

   @Override
   public boolean remove(Object key, Object value) {
      return cache.remove(key, value);
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      return cache.replace(key, oldValue, newValue);
   }

   @Override
   public V replace(K key, V value) {
      return cache.replace(key, value);
   }

   @Override
   public int size() {
      return cache.size();
   }

   @Override
   public boolean isEmpty() {
      return cache.isEmpty();
   }

   @Override
   public boolean containsKey(Object key) {
      return cache.containsKey(key);
   }

   @Override
   public boolean containsValue(Object value) {
      return cache.containsValue(value);
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return cache.compute(key, remappingFunction);
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return cache.compute(key, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.compute(key, remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return cache.computeIfPresent(key, remappingFunction);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return cache.computeIfPresent(key, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.computeIfPresent(key, remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
      return cache.computeIfAbsent(key, mappingFunction);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return computeIfAbsent(key, mappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return computeIfAbsent(key, mappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V get(Object key) {
      return cache.get(key);
   }

   @Override
   public V getOrDefault(Object key, V defaultValue) {
      return cache.getOrDefault(key, defaultValue);
   }

   @Override
   public V put(K key, V value) {
      return cache.put(key, value);
   }

   @Override
   public V remove(Object key) {
      return cache.remove(key);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> t) {
      cache.putAll(t);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return cache.merge(key, value, remappingFunction);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return cache.merge(key, value, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.merge(key, value, remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public void forEach(BiConsumer<? super K, ? super V> action) {
      cache.forEach(action);
   }

   @Override
   @ManagedOperation(
         description = "Clears the cache",
         displayName = "Clears the cache", name = "clear"
   )
   public void clear() {
      cache.clear();
   }

   @Override
   public CacheSet<K> keySet() {
      return cache.keySet();
   }

   @Override
   public CacheSet<Entry<K, V>> entrySet() {
      return cache.entrySet();
   }

   @Override
   public CacheCollection<V> values() {
      return cache.values();
   }

   @Override
   @ManagedOperation(
         description = "Starts the cache.",
         displayName = "Starts cache."
   )
   public void start() {
      cache.start();
   }

   @Override
   @ManagedOperation(
         description = "Stops the cache.",
         displayName = "Stops cache."
   )
   public void stop() {
      cache.stop();
   }

   @Override
   @ManagedOperation(
         description = "Shuts down the cache across the cluster",
         displayName = "Clustered cache shutdown"
   )
   public void shutdown() {
      cache.shutdown();
   }

   @Override
   public void addListener(Object listener) {
      cache.addListener(listener);
   }

   @Override
   public void addListener(Object listener, KeyFilter<? super K> filter) {
      cache.addListener(listener, filter);
   }

   @Override
   public <C> void addListener(Object listener, CacheEventFilter<? super K, ? super V> filter,
                               CacheEventConverter<? super K, ? super V, C> converter) {
      cache.addListener(listener, filter, converter);
   }

   @Override
   public void removeListener(Object listener) {
      cache.removeListener(listener);
   }

   @Override
   public Set<Object> getListeners() {
      return cache.getListeners();
   }

   @Override
   public <C> void addFilteredListener(Object listener,
         CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter,
         Set<Class<? extends Annotation>> filterAnnotations) {
      cache.addFilteredListener(listener, filter, converter, filterAnnotations);
   }

   @Override
   public <C> void addStorageFormatFilteredListener(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter, Set<Class<? extends Annotation>> filterAnnotations) {
      cache.addStorageFormatFilteredListener(listener, filter, converter, filterAnnotations);
   }

   @Override
   public CompletableFuture<V> getAsync(K key) {
      return cache.getAsync(key);
   }

   @Override
   public CompletableFuture<Map<K, V>> getAllAsync(Set<?> keys) {
      return cache.getAllAsync(keys);
   }

   @ManagedAttribute(
         description = "Returns the cache configuration in form of properties",
         displayName = "Cache configuration properties",
         dataType = DataType.TRAIT,
         displayType = DisplayType.SUMMARY
   )
   public Properties getConfigurationAsProperties() {
      return new PropertyFormatter().format(getCacheConfiguration());
   }

   @Override
   public String toString() {
      return cache.toString();
   }

   public Cache<K, V> getDelegate() {
      return cache;
   }

   /**
    * Fully unwraps a given cache returning the base cache.  Will unwrap all <b>AbstractDelegatingCache</b> wrappers.
    * @param cache
    * @param <K>
    * @param <V>
    * @return
    */
   public static <K, V> Cache<K, V> unwrapCache(Cache<K, V> cache) {
      if (cache instanceof AbstractDelegatingCache) {
         return unwrapCache(((AbstractDelegatingCache<K, V>) cache).getDelegate());
      }
      return cache;
   }
}
