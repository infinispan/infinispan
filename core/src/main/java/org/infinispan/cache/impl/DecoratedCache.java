package org.infinispan.cache.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheSet;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.filter.KeyFilter;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.stream.StreamMarshalling;
import org.infinispan.stream.impl.local.ValueCacheCollection;

/**
 * A decorator to a cache, which can be built with a specific {@link ClassLoader} and a set of {@link Flag}s.  This
 * {@link ClassLoader} and set of {@link Flag}s will be applied to all cache invocations made via this decorator.
 * <p/>
 * In addition to cleaner and more readable code, this approach offers a performance benefit to using
 * {@link AdvancedCache#withFlags(org.infinispan.context.Flag...)} API, thanks to
 * internal optimizations that can be made when the {@link Flag} set is unchanging.
 *
 * @author Manik Surtani
 * @author Sanne Grinovero
 * @author Tristan Tarrant
 * @see AdvancedCache#withFlags(org.infinispan.context.Flag...)
 * @since 5.1
 */
public class DecoratedCache<K, V> extends AbstractDelegatingAdvancedCache<K, V> {

   private final long flags;
   private final CacheImpl<K, V> cacheImplementation;

   public DecoratedCache(AdvancedCache<K, V> delegate, Flag... flags) {
      super(delegate);

      if (flags == null)
         throw new IllegalArgumentException("There is no point in using a DecoratedCache if Flags are set.");

      if (flags.length == 0)
         this.flags = EnumUtil.EMPTY_BIT_SET;
      else {
         this.flags = EnumUtil.bitSetOf(flags);
      }

      // Yuk
      cacheImplementation = (CacheImpl<K, V>) delegate;
   }

   private DecoratedCache(CacheImpl<K, V> delegate, long newFlags) {
      //this constructor is private so we already checked for argument validity
      super(delegate);
      this.flags = newFlags;
      this.cacheImplementation = delegate;
   }

   @Override
   public AdvancedCache<K, V> with(final ClassLoader classLoader) {
      if (classLoader == null) throw new IllegalArgumentException("ClassLoader cannot be null!");
      return this;
   }

   @Override
   public AdvancedCache<K, V> withFlags(final Flag... flags) {
      if (flags == null || flags.length == 0)
         return this;
      else {
         long newFlags = EnumUtil.bitSetOf(flags);
         if (EnumUtil.containsAll(this.flags, newFlags)) {
            //we already have all specified flags
            return this;
         } else {
            return new DecoratedCache<>(this.cacheImplementation, EnumUtil.mergeBitSets(this.flags, newFlags));
         }
      }
   }

   @Override
   public ClassLoader getClassLoader() {
      return cacheImplementation.getClassLoader();
   }

   @Override
   public void stop() {
      cacheImplementation.stop();
   }

   @Override
   public boolean lock(K... keys) {
      return cacheImplementation.lock(Arrays.asList(keys), flags);
   }

   @Override
   public boolean lock(Collection<? extends K> keys) {
      return cacheImplementation.lock(keys, flags);
   }

   @Override
   public void putForExternalRead(K key, V value) {
      cacheImplementation.putForExternalRead(key, value, flags);
   }

   @Override
   public void putForExternalRead(K key, V value, Metadata metadata) {
      cacheImplementation.putForExternalRead(key, value, metadata, flags);
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
       .lifespan(lifespan, unit)
       .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
       .build();

      cacheImplementation.putForExternalRead(key, value, metadata, flags);
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
       .lifespan(lifespan, lifespanUnit)
       .maxIdle(maxIdle, maxIdleUnit)
       .build();

      cacheImplementation.putForExternalRead(key, value, metadata, flags);
   }

   @Override
   public void evict(K key) {
      cacheImplementation.evict(key, flags);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, unit)
            .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
            .build();

      return cacheImplementation.put(key, value, metadata, flags);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, unit)
            .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
            .build();

      return cacheImplementation.putIfAbsent(key, value, metadata, flags);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, unit)
            .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
            .build();
      cacheImplementation.putAll(map, metadata, flags);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, unit)
            .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
            .build();

      return cacheImplementation.replace(key, value, metadata, flags);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, unit)
            .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
            .build();

      return cacheImplementation.replace(key, oldValue, value, metadata, flags);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, maxIdleTimeUnit)
            .build();

      return cacheImplementation.put(key, value, metadata, flags);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, maxIdleTimeUnit)
            .build();

      return cacheImplementation.putIfAbsent(key, value, metadata, flags);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, maxIdleTimeUnit)
            .build();
      cacheImplementation.putAll(map, metadata, flags);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, maxIdleTimeUnit)
            .build();

      return cacheImplementation.replace(key, value, metadata, flags);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, maxIdleTimeUnit)
            .build();

      return cacheImplementation.replace(key, oldValue, value, metadata, flags);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value) {
      return cacheImplementation.putAsync(key, value, cacheImplementation.defaultMetadata, flags);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, unit)
            .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
            .build();

      return cacheImplementation.putAsync(key, value, metadata, flags);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit)
            .build();

      return cacheImplementation.putAsync(key, value, metadata, flags);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return cacheImplementation.putAllAsync(data, cacheImplementation.defaultMetadata, flags);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, unit)
            .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
            .build();
      return cacheImplementation.putAllAsync(data, metadata, flags);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit)
            .build();
      return cacheImplementation.putAllAsync(data, metadata, flags);
   }

   @Override
   public CompletableFuture<Void> clearAsync() {
      return cacheImplementation.clearAsync(flags);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value) {
      return cacheImplementation.putIfAbsentAsync(key, value, cacheImplementation.defaultMetadata, flags);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, unit)
            .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
            .build();

      return cacheImplementation.putIfAbsentAsync(key, value, metadata, flags);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit)
            .build();

      return cacheImplementation.putIfAbsentAsync(key, value, metadata, flags);
   }

   @Override
   public CompletableFuture<V> removeAsync(Object key) {
      return cacheImplementation.removeAsync(key, flags);
   }

   @Override
   public CompletableFuture<Boolean> removeAsync(Object key, Object value) {
      return cacheImplementation.removeAsync(key, value, flags);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value) {
      return cacheImplementation.replaceAsync(key, value, cacheImplementation.defaultMetadata, flags);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, unit)
            .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
            .build();

      return cacheImplementation.replaceAsync(key, value, metadata, flags);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit)
            .build();

      return cacheImplementation.replaceAsync(key, value, metadata, flags);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return cacheImplementation.replaceAsync(key, oldValue, newValue, cacheImplementation.defaultMetadata, flags);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, unit)
            .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
            .build();

      return cacheImplementation.replaceAsync(key, oldValue, newValue, metadata, flags);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit)
            .build();

      return cacheImplementation.replaceAsync(key, oldValue, newValue, metadata, flags);
   }

   @Override
   public CompletableFuture<V> getAsync(K key) {
      return cacheImplementation.getAsync(key, flags);
   }

   @Override
   public int size() {
      return cacheImplementation.size(flags);
   }

   @Override
   public boolean isEmpty() {
      return cacheImplementation.isEmpty(flags);
   }

   @Override
   public boolean containsKey(Object key) {
      return cacheImplementation.containsKey(key, flags);
   }

   @Override
   public boolean containsValue(Object value) {
      Objects.nonNull(value);
      return values().stream().anyMatch(StreamMarshalling.equalityPredicate(value));
   }

   @Override
   public V get(Object key) {
      return cacheImplementation.get(key, flags);
   }

   @Override
   public Map<K, V> getAll(Set<?> keys) {
      return cacheImplementation.getAll(keys, flags);
   }

   @Override
   public V put(K key, V value) {
      return cacheImplementation.put(key, value, cacheImplementation.defaultMetadata, flags);
   }

   @Override
   public V remove(Object key) {
      return cacheImplementation.remove(key, flags);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, Metadata metadata) {
      cacheImplementation.putAll(map, metadata, flags);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m) {
      cacheImplementation.putAll(m, cacheImplementation.defaultMetadata, flags);
   }

   @Override
   public void clear() {
      cacheImplementation.clear(flags);
   }

   @Override
   public CacheSet<K> keySet() {
      return cacheImplementation.keySet(flags);
   }

   @Override
   public Map<K, V> getGroup(String groupName) {
      return cacheImplementation.getGroup(groupName, flags);
   }

   @Override
   public void removeGroup(String groupName) {
      cacheImplementation.removeGroup(groupName, flags);
   }

   @Override
   public CacheCollection<V> values() {
      return new ValueCacheCollection<>(this, cacheEntrySet());
   }

   @Override
   public CacheSet<Entry<K, V>> entrySet() {
      return cacheImplementation.entrySet(flags);
   }

   @Override
   public CacheSet<CacheEntry<K, V>> cacheEntrySet() {
      return cacheImplementation.cacheEntrySet(flags);
   }

   @Override
   public V putIfAbsent(K key, V value) {
      return cacheImplementation.putIfAbsent(key, value, cacheImplementation.defaultMetadata, flags);
   }

   @Override
   public boolean remove(Object key, Object value) {
      return cacheImplementation.remove(key, value, flags);
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      return cacheImplementation.replace(key, oldValue, newValue, cacheImplementation.defaultMetadata, flags);
   }

   @Override
   public V replace(K key, V value) {
      return cacheImplementation.replace(key, value, cacheImplementation.defaultMetadata, flags);
   }

   //Not exposed on interface
   public long getFlagsBitSet() {
      return flags;
   }

   @Override
   public void addListener(Object listener) {
      cacheImplementation.notifier.addListener(listener, (ClassLoader) null);
   }

   @Override
   public void addListener(Object listener, KeyFilter<? super K> filter) {
      cacheImplementation.notifier.addListener(listener, filter, null);
   }

   @Override
   public V put(K key, V value, Metadata metadata) {
      return cacheImplementation.put(key, value, metadata, flags);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, Metadata metadata) {
      return cacheImplementation.putAsync(key, value, metadata, flags);
   }

   @Override
   public V putIfAbsent(K key, V value, Metadata metadata) {
      return cacheImplementation.putIfAbsent(key, value, metadata, flags);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, Metadata metadata) {
      return cacheImplementation.replace(key, oldValue, value, metadata, flags);
   }

   @Override
   public V replace(K key, V value, Metadata metadata) {
      return cacheImplementation.replace(key, value, metadata, flags);
   }

   @Override
   public CacheEntry getCacheEntry(Object key) {
      return cacheImplementation.getCacheEntry(key, flags);
   }
}
