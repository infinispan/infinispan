package org.infinispan.cache.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheSet;
import org.infinispan.LockedStream;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.filter.KeyFilter;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.stream.StreamMarshalling;
import org.infinispan.stream.impl.local.ValueCacheCollection;

/**
 * A decorator to a cache, which can be built with a specific set of {@link Flag}s.  This
 * set of {@link Flag}s will be applied to all cache invocations made via this decorator.
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

   private static final Flag[] EMPTY_FLAGS = new Flag[0];
   private final long flags;
   private final Object lockOwner;
   private final CacheImpl<K, V> cacheImplementation;

   public DecoratedCache(AdvancedCache<K, V> delegate) {
      this(delegate, EMPTY_FLAGS);
   }

   public DecoratedCache(AdvancedCache<K, V> delegate, Flag... flags) {
      this(delegate, null, flags);
   }

   public DecoratedCache(AdvancedCache<K, V> delegate, Object lockOwner, Flag... flags) {
      super(delegate);
      if (flags.length == 0)
         this.flags = EnumUtil.EMPTY_BIT_SET;
      else {
         this.flags = EnumUtil.bitSetOf(flags);
      }

      this.lockOwner = lockOwner;

      // Yuk
      cacheImplementation = (CacheImpl<K, V>) delegate;
   }

   private DecoratedCache(CacheImpl<K, V> delegate, Object lockOwner, long newFlags) {
      //this constructor is private so we already checked for argument validity
      super(delegate);
      this.flags = newFlags;
      this.lockOwner = lockOwner;
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
            return new DecoratedCache<>(this.cacheImplementation, lockOwner, EnumUtil.mergeBitSets(this.flags, newFlags));
         }
      }
   }

   @Override
   public AdvancedCache<K, V> lockAs(Object lockOwner) {
      Objects.requireNonNull(lockOwner);
      if (lockOwner != this.lockOwner) {
         return new DecoratedCache<>(cacheImplementation, lockOwner, flags);
      }
      return this;
   }

   public Object getLockOwner() {
      return lockOwner;
   }

   @Override
   public LockedStream<K, V> lockedStream() {
      assertNoLockOwner("lockedStream");
      return super.lockedStream();
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
      assertNoLockOwner("lock");
      return cacheImplementation.lock(Arrays.asList(keys), flags);
   }

   @Override
   public boolean lock(Collection<? extends K> keys) {
      assertNoLockOwner("lock");
      return cacheImplementation.lock(keys, flags);
   }

   @Override
   public void putForExternalRead(K key, V value) {
      putForExternalRead(key, value, cacheImplementation.defaultMetadata);
   }

   @Override
   public void putForExternalRead(K key, V value, Metadata metadata) {
      assertNoLockOwner("putForExternalRead");
      cacheImplementation.putForExternalRead(key, value, metadata, flags);
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
       .lifespan(lifespan, unit)
       .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
       .build();

      putForExternalRead(key, value, metadata);
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
       .lifespan(lifespan, lifespanUnit)
       .maxIdle(maxIdle, maxIdleUnit)
       .build();

      putForExternalRead(key, value, metadata);
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

      return put(key, value, metadata);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, unit)
            .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
            .build();

      return putIfAbsent(key, value, metadata);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, unit)
            .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
            .build();
      putAll(map, metadata);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, unit)
            .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
            .build();

      return replace(key, value, metadata);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, unit)
            .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
            .build();

      return replace(key, oldValue, value, metadata);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, maxIdleTimeUnit)
            .build();

      return put(key, value, metadata);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, maxIdleTimeUnit)
            .build();

      return putIfAbsent(key, value, metadata);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, maxIdleTimeUnit)
            .build();
      putAll(map, metadata);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, maxIdleTimeUnit)
            .build();

      return replace(key, value, metadata);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, maxIdleTimeUnit)
            .build();

      return replace(key, oldValue, value, metadata);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value) {
      return putAsync(key, value, cacheImplementation.defaultMetadata);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, unit)
            .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
            .build();

      return putAsync(key, value, metadata);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit)
            .build();

      return putAsync(key, value, metadata);
   }

   private void assertNoLockOwner(String name) {
      if (lockOwner != null) {
         throw new IllegalStateException(name + " method cannot be used when a lock owner is configured");
      }
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return putAllAsync(data, cacheImplementation.defaultMetadata);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, unit)
            .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
            .build();
      return putAllAsync(data, metadata);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit)
            .build();
      return putAllAsync(data, metadata);
   }

   CompletableFuture<Void> putAllAsync(final Map<? extends K, ? extends V> data, final Metadata metadata) {
      return cacheImplementation.putAllAsync(data, metadata, flags, writeContext(data.size()));
   }

   @Override
   public CompletableFuture<Void> clearAsync() {
      return cacheImplementation.clearAsync(flags);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value) {
      return putIfAbsentAsync(key, value, cacheImplementation.defaultMetadata);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, unit)
            .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
            .build();

      return putIfAbsentAsync(key, value, metadata);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit)
            .build();

      return putIfAbsentAsync(key, value, metadata);
   }

   CompletableFuture<V> putIfAbsentAsync(final K key, final V value, final Metadata metadata) {
      return cacheImplementation.putIfAbsentAsync(key, value, metadata, flags, writeContext(1));
   }

   @Override
   public CompletableFuture<V> removeAsync(Object key) {
      return cacheImplementation.removeAsync(key, flags, writeContext(1));
   }

   @Override
   public CompletableFuture<Boolean> removeAsync(Object key, Object value) {
      return cacheImplementation.removeAsync(key, value, flags, writeContext(1));
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value) {
      return replaceAsync(key, value, cacheImplementation.defaultMetadata);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, unit)
            .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
            .build();

      return replaceAsync(key, value, metadata);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit)
            .build();

      return replaceAsync(key, value, metadata);
   }

   CompletableFuture<V> replaceAsync(final K key, final V value, final Metadata metadata) {
      return cacheImplementation.replaceAsync(key, value, metadata, flags, writeContext(1));
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return replaceAsync(key, oldValue, newValue, cacheImplementation.defaultMetadata);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, unit)
            .maxIdle(cacheImplementation.defaultMetadata.maxIdle(), MILLISECONDS)
            .build();

      return replaceAsync(key, oldValue, newValue, metadata);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdle, maxIdleUnit)
            .build();

      return replaceAsync(key, oldValue, newValue, metadata);
   }

   CompletableFuture<Boolean> replaceAsync(final K key, final V oldValue, final V newValue,
         final Metadata metadata) {
      return cacheImplementation.replaceAsync(key, oldValue, newValue, metadata, flags, writeContext(1));
   }

   @Override
   public CompletableFuture<V> getAsync(K key) {
      return cacheImplementation.getAsync(key, flags, readContext(1));
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
      return cacheImplementation.containsKey(key, flags, readContext(1));
   }

   @Override
   public boolean containsValue(Object value) {
      Objects.requireNonNull(value);
      return values().stream().anyMatch(StreamMarshalling.equalityPredicate(value));
   }

   @Override
   public V get(Object key) {
      return cacheImplementation.get(key, flags, readContext(1));
   }

   @Override
   public Map<K, V> getAll(Set<?> keys) {
      return cacheImplementation.getAll(keys, flags, readContext(keys.size()));
   }

   @Override
   public Map<K, CacheEntry<K, V>> getAllCacheEntries(Set<?> keys) {
      return cacheImplementation.getAllCacheEntries(keys, flags, readContext(keys.size()));
   }

   @Override
   public V put(K key, V value) {
      return put(key, value, cacheImplementation.defaultMetadata);
   }

   @Override
   public V remove(Object key) {
      return cacheImplementation.remove(key, flags, writeContext(1));
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, Metadata metadata) {
      cacheImplementation.putAll(map, metadata, flags, writeContext(map.size()));
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m) {
      putAll(m, cacheImplementation.defaultMetadata);
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
      assertNoLockOwner("removeGroup");
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
      return cacheImplementation.cacheEntrySet(flags, readContext(-1));
   }

   @Override
   public V putIfAbsent(K key, V value) {
      return putIfAbsent(key, value, cacheImplementation.defaultMetadata);
   }

   @Override
   public boolean remove(Object key, Object value) {
      return cacheImplementation.remove(key, value, flags, writeContext(1));
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      return replace(key, oldValue, newValue, cacheImplementation.defaultMetadata);
   }

   @Override
   public V replace(K key, V value) {
      return replace(key, value, cacheImplementation.defaultMetadata);
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return compute(key, remappingFunction, cacheImplementation.defaultMetadata);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return computeIfPresent(key, remappingFunction, cacheImplementation.defaultMetadata);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
      return computeIfAbsent(key, mappingFunction, cacheImplementation.defaultMetadata);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return merge(key, value, remappingFunction, cacheImplementation.defaultMetadata);
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
      return cacheImplementation.put(key, value, metadata, flags, writeContext(1));
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, Metadata metadata) {
      return cacheImplementation.putAsync(key, value, metadata, flags, writeContext(1));
   }

   @Override
   public V putIfAbsent(K key, V value, Metadata metadata) {
      return cacheImplementation.putIfAbsent(key, value, metadata, flags, writeContext(1));
   }

   @Override
   public boolean replace(K key, V oldValue, V value, Metadata metadata) {
      return cacheImplementation.replace(key, oldValue, value, metadata, flags, writeContext(1));
   }

   @Override
   public V replace(K key, V value, Metadata metadata) {
      return cacheImplementation.replace(key, value, metadata, flags, writeContext(1));
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return cacheImplementation.computeInternal(key, remappingFunction, false, metadata, flags, writeContext(1));
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return cacheImplementation.computeInternal(key, remappingFunction, true, metadata, flags, writeContext(1));
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata) {
      return cacheImplementation.computeIfAbsentInternal(key, mappingFunction, metadata, flags, writeContext(1));
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return cacheImplementation.mergeInternal(key, value, remappingFunction, metadata, flags, writeContext(1));
   }

   @Override
   public CacheEntry getCacheEntry(Object key) {
      return cacheImplementation.getCacheEntry(key, flags, readContext(1));
   }

   protected InvocationContext readContext(int size) {
      InvocationContext ctx = cacheImplementation.invocationContextFactory.createInvocationContext(false, size);
      if (lockOwner != null) {
         ctx.setLockOwner(lockOwner);
      }
      return ctx;
   }

   protected InvocationContext writeContext(int size) {
      InvocationContext ctx = cacheImplementation.getInvocationContextWithImplicitTransaction(false, size);
      if (lockOwner != null) {
         ctx.setLockOwner(lockOwner);
      }
      return ctx;
   }
}
