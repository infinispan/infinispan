package org.infinispan.functional.decorators;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.atomic.Delta;
import org.infinispan.batch.BatchContainer;
import org.infinispan.commons.api.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.commons.api.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.commons.api.functional.MetaParam.MetaLifespan;
import org.infinispan.commons.api.functional.MetaParam.MetaMaxIdle;
import org.infinispan.commons.api.functional.Param.FutureMode;
import org.infinispan.commons.api.functional.Param.PersistenceMode;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.filter.KeyFilter;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.interceptors.SequentialInterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
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

import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.infinispan.commons.marshall.MarshallableFunctions.*;

public final class FunctionalAdvancedCache<K, V> implements AdvancedCache<K, V> {

   final AdvancedCache<K, V> cache;

   final ConcurrentMap<K, V> map;
   final ReadWriteMap<K, V> rw;
   final ReadWriteMap<K, V> rwCompleted;
   final WriteOnlyMap<K, V> wo;
   final WriteOnlyMap<K, V> woCompleted;

   private FunctionalAdvancedCache(ConcurrentMap<K, V> map, AdvancedCache<K, V> cache) {
      this.map = map;
      this.cache = cache;
      FunctionalMapImpl<K, V> fmap = FunctionalMapImpl.create(cache);
      this.rw = ReadWriteMapImpl.create(fmap);
      this.wo = WriteOnlyMapImpl.create(fmap);
      this.woCompleted = wo.withParams(FutureMode.COMPLETED);
      this.rwCompleted = rw.withParams(FutureMode.COMPLETED);
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
      return await(rwCompleted.eval(key, value, setValueMetasReturnPrevOrNull(metaLifespan, metaMaxIdle)));
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      final MetaLifespan metaLifespan = createMetaLifespan(lifespan, lifespanUnit);
      final MetaMaxIdle metaMaxIdle = createMetaMaxIdle(maxIdleTime, maxIdleTimeUnit);
      await(woCompleted.evalMany(map, setValueMetasConsumer(metaLifespan, metaMaxIdle)));
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      final MetaLifespan metaLifespan = createMetaLifespan(lifespan, lifespanUnit);
      final MetaMaxIdle metaMaxIdle = createMetaMaxIdle(maxIdleTime, maxIdleTimeUnit);
      return await(rwCompleted.eval(key, value, setValueMetasIfAbsentReturnPrevOrNull(metaLifespan, metaMaxIdle)));
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      final MetaLifespan metaLifespan = createMetaLifespan(lifespan, lifespanUnit);
      final MetaMaxIdle metaMaxIdle = createMetaMaxIdle(maxIdleTime, maxIdleTimeUnit);
      return await(rwCompleted.eval(key, value, setValueMetasIfPresentReturnPrevOrNull(metaLifespan, metaMaxIdle)));
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      final MetaLifespan metaLifespan = createMetaLifespan(lifespan, lifespanUnit);
      final MetaMaxIdle metaMaxIdle = createMetaMaxIdle(maxIdleTime, maxIdleTimeUnit);
      return await(rwCompleted.eval(key, value, setValueIfEqualsReturnBoolean(oldValue, metaLifespan, metaMaxIdle)));
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      final MetaLifespan metaLifespan = createMetaLifespan(lifespan, unit);
      return await(rwCompleted.eval(key, value, setValueMetasReturnPrevOrNull(metaLifespan)));
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      final MetaLifespan metaLifespan = createMetaLifespan(lifespan, unit);
      await(woCompleted.evalMany(map, setValueMetasConsumer(metaLifespan)));
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      final MetaLifespan metaLifespan = createMetaLifespan(lifespan, unit);
      return await(rwCompleted.eval(key, value, setValueMetasIfAbsentReturnPrevOrNull(metaLifespan)));
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      final MetaLifespan metaLifespan = createMetaLifespan(lifespan, unit);
      return await(rwCompleted.eval(key, value, setValueMetasIfPresentReturnPrevOrNull(metaLifespan)));
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      final MetaLifespan metaLifespan = createMetaLifespan(lifespan, unit);
      return await(rwCompleted.eval(key, value, setValueIfEqualsReturnBoolean(oldValue, metaLifespan)));
   }

   @Override
   public void evict(K key) {
      await(woCompleted.withParams(PersistenceMode.SKIP).eval(key, removeConsumer()));
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
   public boolean addInterceptorBefore(CommandInterceptor i, Class<? extends CommandInterceptor> beforeInterceptor) {
      return cache.addInterceptorBefore(i, beforeInterceptor);
   }

   @Override
   public AdvancedCache<K, V> withFlags(Flag... flags) {
      return cache.withFlags(flags);
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

   ////////////////////////////////////////////////////////////////////////////

   @Override
   public void addInterceptor(CommandInterceptor i, int position) {
      // TODO: Customise this generated block
   }

   @Override
   public SequentialInterceptorChain getSequentialInterceptorChain() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean addInterceptorAfter(CommandInterceptor i, Class<? extends CommandInterceptor> afterInterceptor) {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public void removeInterceptor(int position) {
      // TODO: Customise this generated block
   }

   @Override
   public void removeInterceptor(Class<? extends CommandInterceptor> interceptorType) {
      // TODO: Customise this generated block
   }

   @Override
   public List<CommandInterceptor> getInterceptorChain() {
      return null;  // TODO: Customise this generated block
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
   public boolean lock(K... keys) {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public boolean lock(Collection<? extends K> keys) {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public void applyDelta(K deltaAwareValueKey, Delta delta, Object... locksToAcquire) {
      // TODO: Customise this generated block
   }

   @Override
   public BatchContainer getBatchContainer() {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public InvocationContextContainer getInvocationContextContainer() {
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
   public CompletableFuture<V> putAsync(K key, V value, Metadata metadata) {
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
   public void removeExpired(K key, V value, Long lifespan) {
      // TODO: Customise this generated block
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
   public boolean startBatch() {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public void endBatch(boolean successful) {
      // TODO: Customise this generated block
   }

   @Override
   public void addListener(Object listener, KeyFilter<? super K> filter) {
      // TODO: Customise this generated block
   }

   @Override
   public <C> void addListener(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter) {
      // TODO: Customise this generated block
   }

   @Override
   public void addListener(Object listener) {
      // TODO: Customise this generated block
   }

   @Override
   public void removeListener(Object listener) {
      // TODO: Customise this generated block
   }

   @Override
   public Set<Object> getListeners() {
      return null;  // TODO: Customise this generated block
   }

   public static <T> T await(CompletableFuture<T> cf) {
      try {
         return cf.get();
      } catch (InterruptedException | ExecutionException e) {
         throw new Error(e);
      }
   }

   private static final class SetAsCacheSet<E> implements CacheSet<E> {
      final Set<E> set;

      private SetAsCacheSet(Set<E> set) {
         this.set = set;
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
      public int size() {
         return set.size();
      }

      @Override
      public boolean isEmpty() {
         return set.isEmpty();
      }

      @Override
      public boolean contains(Object o) {
         return set.contains(o);
      }

      @Override
      public CloseableIterator<E> iterator() {
         return Closeables.iterator(set.iterator());
      }

      @Override
      public Object[] toArray() {
         return set.toArray();
      }

      @Override
      public <T> T[] toArray(T[] a) {
         return set.toArray(a);
      }

      @Override
      public boolean add(E e) {
         return set.add(e);
      }

      @Override
      public boolean remove(Object o) {
         return set.remove(o);
      }

      @Override
      public boolean containsAll(Collection<?> c) {
         return set.containsAll(c);
      }

      @Override
      public boolean addAll(Collection<? extends E> c) {
         return set.addAll(c);
      }

      @Override
      public boolean removeAll(Collection<?> c) {
         return set.removeAll(c);
      }

      @Override
      public boolean retainAll(Collection<?> c) {
         return set.retainAll(c);
      }

      @Override
      public void clear() {
         set.clear();
      }

      @Override
      public CloseableSpliterator<E> spliterator() {
         return null;
      }

      @Override
      public String toString() {
         return "SetAsCacheSet{" +
            "set=" + set +
            '}';
      }
   }

   private static class CollectionAsCacheCollection<E> implements CacheCollection<E> {
      private final Collection<E> col;

      public CollectionAsCacheCollection(Collection<E> col) {
         this.col = col;
      }

      @Override
      public int size() {
         return col.size();
      }

      @Override
      public boolean isEmpty() {
         return col.isEmpty();
      }

      @Override
      public boolean contains(Object o) {
         return col.contains(o);
      }

      @Override
      public CloseableIterator<E> iterator() {
         return Closeables.iterator(col.iterator());
      }

      @Override
      public CloseableSpliterator<E> spliterator() {
         return null;  // TODO: Customise this generated block
      }

      @Override
      public Object[] toArray() {
         return col.toArray();
      }

      @Override
      public <T> T[] toArray(T[] a) {
         return col.toArray(a);
      }

      @Override
      public boolean add(E e) {
         return col.add(e);
      }

      @Override
      public boolean remove(Object o) {
         return col.remove(o);
      }

      @Override
      public boolean containsAll(Collection<?> c) {
         return col.containsAll(c);
      }

      @Override
      public boolean addAll(Collection<? extends E> c) {
         return col.addAll(c);
      }

      @Override
      public boolean removeAll(Collection<?> c) {
         return col.removeAll(c);
      }

      @Override
      public boolean retainAll(Collection<?> c) {
         return col.retainAll(c);
      }

      @Override
      public void clear() {
         col.clear();
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
