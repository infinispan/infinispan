package org.infinispan.functional.decorators;

import static org.infinispan.marshall.core.MarshallableFunctions.removeConsumer;
import static org.infinispan.marshall.core.MarshallableFunctions.setValueIfEqualsReturnBoolean;
import static org.infinispan.marshall.core.MarshallableFunctions.setValueMetasConsumer;
import static org.infinispan.marshall.core.MarshallableFunctions.setValueMetasIfAbsentReturnPrevOrNull;
import static org.infinispan.marshall.core.MarshallableFunctions.setValueMetasIfPresentReturnPrevOrNull;
import static org.infinispan.marshall.core.MarshallableFunctions.setValueMetasReturnPrevOrNull;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.security.auth.Subject;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.LockedStream;
import org.infinispan.batch.BatchContainer;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.util.AbstractDelegatingCollection;
import org.infinispan.commons.util.AbstractDelegatingSet;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.encoding.DataConversion;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.functional.MetaParam.MetaLifespan;
import org.infinispan.functional.MetaParam.MetaMaxIdle;
import org.infinispan.functional.Param.PersistenceMode;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.stats.Stats;
import org.infinispan.util.concurrent.locks.LockManager;

public final class FunctionalAdvancedCache<K, V> implements AdvancedCache<K, V> {

   final AdvancedCache<K, V> cache;

   final ConcurrentMap<K, V> map;
   final ReadWriteMap<K, V> rw;
   final WriteOnlyMap<K, V> wo;

   private FunctionalAdvancedCache(ConcurrentMap<K, V> map, AdvancedCache<K, V> cache) {
      this.map = map;
      this.cache = cache;
      FunctionalMapImpl<K, V> fmap = FunctionalMapImpl.create(cache);
      this.rw = ReadWriteMapImpl.create(fmap);
      this.wo = WriteOnlyMapImpl.create(fmap);
   }

   public static <K, V> AdvancedCache<K, V> create(AdvancedCache<K, V> cache) {
      return new FunctionalAdvancedCache<>(FunctionalConcurrentMap.create(cache), cache);
   }

   ////////////////////////////////////////////////////////////////////////////

   @Override
   public V put(K key, V value) {
      return map.put(key, value);
   }

   @Override
   public V get(Object key) {
      return map.get(key);
   }

   @Override
   public V putIfAbsent(K key, V value) {
      return map.putIfAbsent(key, value);
   }

   @Override
   public V replace(K key, V value) {
      return map.replace(key, value);
   }

   @Override
   public V remove(Object key) {
      return map.remove(key);
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      return map.replace(key, oldValue, newValue);
   }

   @Override
   public boolean remove(Object key, Object value) {
      return map.remove(key, value);
   }

   @Override
   public int size() {
      return map.size();
   }

   @Override
   public CompletableFuture<Long> sizeAsync() {
      return cache.sizeAsync();
   }

   @Override
   public CacheSet<Entry<K, V>> entrySet() {
      return new SetAsCacheSet<>(map.entrySet());
   }

   @Override
   public CacheCollection<V> values() {
      return new CollectionAsCacheCollection<>(map.values());
   }

   @Override
   public void clear() {
      map.clear();
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m) {
      map.putAll(m);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      final MetaLifespan metaLifespan = createMetaLifespan(lifespan, lifespanUnit);
      final MetaMaxIdle metaMaxIdle = createMetaMaxIdle(maxIdleTime, maxIdleTimeUnit);
      return await(rw.eval(key, value, setValueMetasReturnPrevOrNull(metaLifespan, metaMaxIdle)));
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      final MetaLifespan metaLifespan = createMetaLifespan(lifespan, lifespanUnit);
      final MetaMaxIdle metaMaxIdle = createMetaMaxIdle(maxIdleTime, maxIdleTimeUnit);
      await(wo.evalMany(map, setValueMetasConsumer(metaLifespan, metaMaxIdle)));
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      final MetaLifespan metaLifespan = createMetaLifespan(lifespan, lifespanUnit);
      final MetaMaxIdle metaMaxIdle = createMetaMaxIdle(maxIdleTime, maxIdleTimeUnit);
      return await(rw.eval(key, value, setValueMetasIfAbsentReturnPrevOrNull(metaLifespan, metaMaxIdle)));
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      final MetaLifespan metaLifespan = createMetaLifespan(lifespan, lifespanUnit);
      final MetaMaxIdle metaMaxIdle = createMetaMaxIdle(maxIdleTime, maxIdleTimeUnit);
      return await(rw.eval(key, value, setValueMetasIfPresentReturnPrevOrNull(metaLifespan, metaMaxIdle)));
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      final MetaLifespan metaLifespan = createMetaLifespan(lifespan, lifespanUnit);
      final MetaMaxIdle metaMaxIdle = createMetaMaxIdle(maxIdleTime, maxIdleTimeUnit);
      return await(rw.eval(key, value, setValueIfEqualsReturnBoolean(oldValue, metaLifespan, metaMaxIdle)));
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      final MetaLifespan metaLifespan = createMetaLifespan(lifespan, unit);
      return await(rw.eval(key, value, setValueMetasReturnPrevOrNull(metaLifespan)));
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      final MetaLifespan metaLifespan = createMetaLifespan(lifespan, unit);
      await(wo.evalMany(map, setValueMetasConsumer(metaLifespan)));
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      final MetaLifespan metaLifespan = createMetaLifespan(lifespan, unit);
      return await(rw.eval(key, value, setValueMetasIfAbsentReturnPrevOrNull(metaLifespan)));
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      final MetaLifespan metaLifespan = createMetaLifespan(lifespan, unit);
      return await(rw.eval(key, value, setValueMetasIfPresentReturnPrevOrNull(metaLifespan)));
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      final MetaLifespan metaLifespan = createMetaLifespan(lifespan, unit);
      return await(rw.eval(key, value, setValueIfEqualsReturnBoolean(oldValue, metaLifespan)));
   }

   @Override
   public void evict(K key) {
      await(wo.withParams(PersistenceMode.SKIP).eval(key, removeConsumer()));
   }

   @Override
   public void putForExternalRead(K key, V value) {
      map.putIfAbsent(key, value);
   }

   private MetaLifespan createMetaLifespan(long lifespan, TimeUnit lifespanUnit) {
      return new MetaLifespan(lifespanUnit.toMillis(lifespan));
   }

   private MetaMaxIdle createMetaMaxIdle(long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new MetaMaxIdle(maxIdleTimeUnit.toMillis(maxIdleTime));
   }

   ////////////////////////////////////////////////////////////////////////////

   @Override
   public RpcManager getRpcManager() {
      return cache.getRpcManager();
   }

   @Override
   public ComponentRegistry getComponentRegistry() {
      return cache.getComponentRegistry();
   }

   @Override
   public AdvancedCache<K, V> getAdvancedCache() {
      return cache.getAdvancedCache();
   }

   @Override
   public EmbeddedCacheManager getCacheManager() {
      return cache.getCacheManager();
   }

   @Override
   public AdvancedCache<K, V> withFlags(Flag... flags) {
      return cache.withFlags(flags);
   }

   @Override
   public AdvancedCache<K, V> withSubject(Subject subject) {
      return cache.withSubject(subject);
   }

   @Override
   public Configuration getCacheConfiguration() {
      return cache.getCacheConfiguration();
   }

   @Override
   public void stop() {
      cache.stop();
   }

   @Override
   public void start() {
      cache.start();
   }

   @Override
   public CompletionStage<Boolean> touch(Object key, int segment, boolean touchEvenIfExpired) {
      return cache.touch(key, segment, touchEvenIfExpired);
   }

   @Override
   public CompletionStage<Boolean> touch(Object key, boolean touchEvenIfExpired) {
      return cache.touch(key, -1, touchEvenIfExpired);
   }

   ////////////////////////////////////////////////////////////////////////////

   /**
    * @deprecated Since 10.0, will be removed without a replacement
    */
   @Deprecated
   @Override
   public AsyncInterceptorChain getAsyncInterceptorChain() {
      return cache.getAsyncInterceptorChain();
   }

   @Override
   public EvictionManager getEvictionManager() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public ExpirationManager<K, V> getExpirationManager() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public DistributionManager getDistributionManager() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public AuthorizationManager getAuthorizationManager() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public AdvancedCache<K, V> lockAs(Object lockOwner) {
      throw new UnsupportedOperationException("lockAs is not supported with Functional Cache!");
   }

   @Override
   public boolean lock(K... keys) {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public boolean lock(Collection<? extends K> keys) {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public BatchContainer getBatchContainer() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public DataContainer<K, V> getDataContainer() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public TransactionManager getTransactionManager() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public LockManager getLockManager() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public Stats getStats() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public XAResource getXAResource() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public ClassLoader getClassLoader() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public AdvancedCache<K, V> with(ClassLoader classLoader) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public V put(K key, V value, Metadata metadata) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, Metadata metadata) {
      // TODO: Customise this generated block
   }

   @Override
   public V replace(K key, V value, Metadata metadata) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> replaceAsyncEntry(K key, V value, Metadata metadata) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue, Metadata metadata) {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public V putIfAbsent(K key, V value, Metadata metadata) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public void putForExternalRead(K key, V value, Metadata metadata) {
      // TODO: Customise this generated block
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      throw new UnsupportedOperationException();
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      throw new UnsupportedOperationException();
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      throw new UnsupportedOperationException();
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      throw new UnsupportedOperationException();
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      throw new UnsupportedOperationException();
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      throw new UnsupportedOperationException();
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
      throw new UnsupportedOperationException();
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata) {
      throw new UnsupportedOperationException();
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, Metadata metadata) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> putAsyncEntry(K key, V value, Metadata metadata) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public Map<K, V> getAll(Set<?> keys) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CacheEntry<K, V> getCacheEntry(Object key) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public Map<K, CacheEntry<K, V>> getAllCacheEntries(Set<?> keys) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public Map<K, V> getGroup(String groupName) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public void removeGroup(String groupName) {
      // TODO: Customise this generated block
   }

   @Override
   public AvailabilityMode getAvailability() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public void setAvailability(AvailabilityMode availabilityMode) {
      // TODO: Customise this generated block
   }

   @Override
   public CacheSet<CacheEntry<K, V>> cacheEntrySet() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public LockedStream<K, V> lockedStream() {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<Boolean> removeLifespanExpired(K key, V value, Long lifespan) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletableFuture<Boolean> removeMaxIdleExpired(K key, V value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public AdvancedCache<?, ?> withEncoding(Class<? extends Encoder> encoder) {
      return cache.withEncoding(encoder);
   }

   @Override
   public AdvancedCache<?, ?> withKeyEncoding(Class<? extends Encoder> encoder) {
      return cache.withKeyEncoding(encoder);
   }

   @Override
   public AdvancedCache<?, ?> withEncoding(Class<? extends Encoder> keyEncoder, Class<? extends Encoder> valueEncoder) {
      return cache.withEncoding(keyEncoder, valueEncoder);
   }

   @Override
   public AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> keyWrapper, Class<? extends Wrapper> valueWrapper) {
      return cache.withWrapping(keyWrapper, valueWrapper);
   }

   @Override
   public AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> wrapper) {
      return cache.withWrapping(wrapper);
   }

   @Override
   public AdvancedCache<?, ?> withMediaType(String keyMediaType, String valueMediaType) {
      return cache.withMediaType(keyMediaType, valueMediaType);
   }

   @Override
   public <K1, V1> AdvancedCache<K1, V1> withMediaType(MediaType keyMediaType, MediaType valueMediaType) {
      return cache.withMediaType(keyMediaType, valueMediaType);
   }

   @Override
   public AdvancedCache<K, V> withStorageMediaType() {
      return cache.withStorageMediaType();
   }

   @Override
   public DataConversion getKeyDataConversion() {
      return cache.getKeyDataConversion();
   }

   @Override
   public DataConversion getValueDataConversion() {
      return cache.getValueDataConversion();
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit unit) {
      // TODO: Customise this generated block
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      // TODO: Customise this generated block
   }

   @Override
   public ComponentStatus getStatus() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public boolean isEmpty() {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public boolean containsKey(Object key) {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public boolean containsValue(Object value) {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public CacheSet<K> keySet() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public String getName() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public String getVersion() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<Void> clearAsync() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> putIfAbsentAsyncEntry(K key, V value, Metadata metadata) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<V> removeAsync(Object key) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<Boolean> removeAsync(Object key, Object value) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<V> getAsync(K key) {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return null;
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return null;
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata) {
      return null;
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return null;
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return null;
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return null;
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return null;
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction) {
      return null;
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return null;
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return null;
   }

   @Override
   public boolean startBatch() {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public void endBatch(boolean successful) {
      // TODO: Customise this generated block
   }

   @Override
   public <C> CompletionStage<Void> addListenerAsync(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter) {
      return null;
   }

   @Override
   public CompletionStage<Void> addListenerAsync(Object listener) {
      // TODO: Customise this generated block
      return null;
   }

   @Override
   public CompletionStage<Void> removeListenerAsync(Object listener) {
      // TODO: Customise this generated block
      return null;
   }

   @Deprecated
   @Override
   public Set<Object> getListeners() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public <C> CompletionStage<Void> addFilteredListenerAsync(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter, Set<Class<? extends Annotation>> filterAnnotations) {
      // TODO: Customise this generated block
      return null;
   }

   @Override
   public <C> CompletionStage<Void> addStorageFormatFilteredListenerAsync(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter, Set<Class<? extends Annotation>> filterAnnotations) {
      // TODO: Customise this generated block
      return null;
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> removeAsyncEntry(Object key) {
      return null; // TODO: Customise this generated block
   }

   public static <T> T await(CompletableFuture<T> cf) {
      try {
         return cf.get();
      } catch (InterruptedException | ExecutionException e) {
         throw new Error(e);
      }
   }

   private static final class SetAsCacheSet<E> extends AbstractDelegatingSet<E> implements CacheSet<E> {
      final Set<E> set;

      private SetAsCacheSet(Set<E> set) {
         this.set = set;
      }

      @Override
      protected Set<E> delegate() {
         return set;
      }

      @Override
      public CacheStream<E> stream() {
         return null;
      }

      @Override
      public CacheStream<E> parallelStream() {
         return null;
      }

      @Override
      public CloseableIterator<E> iterator() {
         return Closeables.iterator(set.iterator());
      }

      @Override
      public CloseableSpliterator<E> spliterator() {
         return Closeables.spliterator(set.spliterator());
      }

      @Override
      public String toString() {
         return "SetAsCacheSet{" +
               "set=" + set +
               '}';
      }
   }

   private static class CollectionAsCacheCollection<E> extends AbstractDelegatingCollection<E> implements CacheCollection<E> {
      private final Collection<E> col;

      public CollectionAsCacheCollection(Collection<E> col) {
         this.col = col;
      }

      @Override
      protected Collection<E> delegate() {
         return col;
      }

      @Override
      public CloseableIterator<E> iterator() {
         return Closeables.iterator(col.iterator());
      }

      @Override
      public CloseableSpliterator<E> spliterator() {
         return Closeables.spliterator(col.spliterator());
      }

      @Override
      public CacheStream<E> stream() {
         return null;
      }

      @Override
      public CacheStream<E> parallelStream() {
         return null;
      }
   }
}
