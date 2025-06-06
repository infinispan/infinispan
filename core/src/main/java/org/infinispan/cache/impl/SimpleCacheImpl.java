package org.infinispan.cache.impl;

import static org.infinispan.util.logging.Log.CONFIG;
import static org.infinispan.util.logging.Log.CONTAINER;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.security.auth.Subject;
import javax.transaction.xa.XAResource;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheCollection;
import org.infinispan.CachePublisher;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.LockedStream;
import org.infinispan.batch.BatchContainer;
import org.infinispan.commons.api.query.ContinuousQuery;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.commons.util.SpliteratorMapper;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.format.PropertyFormatter;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.ImmutableContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryInvalidated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManager;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.reactive.publisher.impl.Notifications;
import org.infinispan.reactive.publisher.impl.SegmentPublisherSupplier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.stats.Stats;
import org.infinispan.stream.impl.local.EntryStreamSupplier;
import org.infinispan.stream.impl.local.KeyStreamSupplier;
import org.infinispan.stream.impl.local.LocalCacheStream;
import org.infinispan.util.DataContainerRemoveIterator;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import jakarta.transaction.TransactionManager;

/**
 * Simple local cache without interceptor stack.
 * The cache still implements {@link AdvancedCache} since it is too troublesome to omit that.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@MBean(objectName = CacheImpl.OBJECT_NAME, description = "Component that represents an individual cache instance.")
@Scope(Scopes.NAMED_CACHE)
public class SimpleCacheImpl<K, V> implements AdvancedCache<K, V>, InternalCache<K, V> {
   private static final Log log = LogFactory.getLog(SimpleCacheImpl.class);

   private static final String NULL_KEYS_NOT_SUPPORTED = "Null keys are not supported!";
   private static final String NULL_VALUES_NOT_SUPPORTED = "Null values are not supported!";
   private static final String NULL_FUNCTION_NOT_SUPPORTED = "Null functions are not supported!";
   private static final Class<? extends Annotation>[] FIRED_EVENTS = new Class[]{
         CacheEntryCreated.class, CacheEntryRemoved.class, CacheEntryVisited.class,
         CacheEntryModified.class, CacheEntriesEvicted.class, CacheEntryInvalidated.class,
         CacheEntryExpired.class};

   private final String name;

   @Inject ComponentRegistry componentRegistry;
   @Inject Configuration configuration;
   @Inject EmbeddedCacheManager cacheManager;
   @Inject InternalDataContainer<K, V> dataContainer;
   @Inject CacheNotifier<K, V> cacheNotifier;
   @Inject TimeService timeService;
   @Inject KeyPartitioner keyPartitioner;

   private Metadata defaultMetadata;
   private boolean hasListeners = false;

   public SimpleCacheImpl(String cacheName) {
      this.name = cacheName;
   }

   @Override
   @ManagedOperation(
         description = "Starts the cache.",
         displayName = "Starts cache."
   )
   public void start() {
      this.defaultMetadata = new EmbeddedMetadata.Builder()
            .lifespan(configuration.expiration().lifespan())
            .maxIdle(configuration.expiration().maxIdle()).build();
      componentRegistry.start();
      componentRegistry.postStart();
   }

   @Override
   @ManagedOperation(
         description = "Stops the cache.",
         displayName = "Stops cache."
   )
   public void stop() {
      if (log.isDebugEnabled())
         log.debugf("Stopping cache %s on %s", getName(), getCacheManager().getAddress());
      dataContainer = null;
      componentRegistry.stop();
   }


   @Override
   public void putForExternalRead(K key, V value) {
      ByRef.Boolean isCreatedRef = new ByRef.Boolean(false);
      putForExternalReadInternal(key, value, defaultMetadata, isCreatedRef);
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit unit) {
      Metadata metadata = createMetadata(lifespan, unit);
      ByRef.Boolean isCreatedRef = new ByRef.Boolean(false);
      putForExternalReadInternal(key, value, metadata, isCreatedRef);
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Metadata metadata = createMetadata(lifespan, lifespanUnit, maxIdle, maxIdleUnit);
      ByRef.Boolean isCreatedRef = new ByRef.Boolean(false);
      putForExternalReadInternal(key, value, metadata, isCreatedRef);
   }

   @Override
   public void putForExternalRead(K key, V value, Metadata metadata) {
      ByRef.Boolean isCreatedRef = new ByRef.Boolean(false);
      putForExternalReadInternal(key, value, applyDefaultMetadata(metadata), isCreatedRef);
   }

   protected void putForExternalReadInternal(K key, V value, Metadata metadata, ByRef.Boolean isCreatedRef) {
      Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
      Objects.requireNonNull(value, NULL_VALUES_NOT_SUPPORTED);
      boolean hasListeners = this.hasListeners;
      getDataContainer().compute(key, (k, oldEntry, factory) -> {
         // entry cannot be marked for removal in DC but it compute does not deal with expiration
         if (isNull(oldEntry)) {
            if (hasListeners) {
               CompletionStages.join(cacheNotifier.notifyCacheEntryCreated(k, value, metadata, true, ImmutableContext.INSTANCE, null));
            }
            isCreatedRef.set(true);
            return factory.create(k, value, metadata);
         } else {
            return oldEntry;
         }
      });
      if (hasListeners && isCreatedRef.get()) {
         CompletionStages.join(cacheNotifier.notifyCacheEntryCreated(key, value, metadata, false, ImmutableContext.INSTANCE, null));
      }
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, Metadata metadata) {
      return CompletableFuture.completedFuture(getAndPutInternal(key, value, applyDefaultMetadata(metadata)));
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> putAsyncEntry(K key, V value, Metadata metadata) {
      return CompletableFuture.completedFuture(getAndPutInternalEntry(key, value, applyDefaultMetadata(metadata)));
   }

   private InternalCacheEntry<K, V> internalGet(Object k) {
      InternalCacheEntry<K, V> e = getDataContainer().peek(k);
      if (e != null && e.canExpire()) {
         long currentTimeMillis = timeService.wallClockTime();
         InternalExpirationManager<K, V> iem = (InternalExpirationManager<K, V>) getExpirationManager();
         if (e.isExpired(currentTimeMillis) &&
               iem.entryExpiredInMemory(e, currentTimeMillis, false).join() == Boolean.TRUE) {
            e = null;
         } else {
            e.touch(currentTimeMillis);
         }
      }
      return e;
   }

   @Override
   public Map<K, V> getAll(Set<?> keys) {
      Map<K, V> map = new HashMap<>(keys.size());
      AggregateCompletionStage<Void> aggregateCompletionStage = null;
      if (hasListeners && cacheNotifier.hasListener(CacheEntryVisited.class)) {
         aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      }
      for (Object k : keys) {
         Objects.requireNonNull(k, NULL_KEYS_NOT_SUPPORTED);
         InternalCacheEntry<K, V> entry = internalGet(k);
         if (entry != null) {
            K key = entry.getKey();
            V value = entry.getValue();
            if (aggregateCompletionStage != null) {
               // Notify each key in parallel, but each key must be notified of the post after the pre completes
               aggregateCompletionStage.dependsOn(
                     cacheNotifier.notifyCacheEntryVisited(key, value, true, ImmutableContext.INSTANCE, null)
                           .thenCompose(ignore -> cacheNotifier.notifyCacheEntryVisited(key, value, false, ImmutableContext.INSTANCE, null)));
            }
            map.put(key, value);
         }
      }
      if (aggregateCompletionStage != null) {
         CompletionStages.join(aggregateCompletionStage.freeze());
      }
      return map;
   }

   @Override
   public CompletableFuture<Map<K, V>> getAllAsync(Set<?> keys) {
      return CompletableFuture.completedFuture(getAll(keys));
   }

   @Override
   public CacheEntry<K, V> getCacheEntry(Object k) {
      InternalCacheEntry<K, V> entry = internalGet(k);
      if (entry != null) {
         K key = entry.getKey();
         V value = entry.getValue();
         if (hasListeners) {
            CompletionStages.join(cacheNotifier.notifyCacheEntryVisited(key, value, true, ImmutableContext.INSTANCE, null));
            CompletionStages.join(cacheNotifier.notifyCacheEntryVisited(key, value, false, ImmutableContext.INSTANCE, null));
         }
      }
      return entry;
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> getCacheEntryAsync(Object key) {
      return CompletableFuture.completedFuture(getCacheEntry(key));
   }

   @Override
   public Map<K, CacheEntry<K, V>> getAllCacheEntries(Set<?> keys) {
      Map<K, CacheEntry<K, V>> map = new HashMap<>(keys.size());
      AggregateCompletionStage<Void> aggregateCompletionStage = null;
      if (hasListeners && cacheNotifier.hasListener(CacheEntryVisited.class)) {
         aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      }
      for (Object key : keys) {
         Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
         InternalCacheEntry<K, V> entry = internalGet(key);
         if (entry != null) {
            V value = entry.getValue();
            if (aggregateCompletionStage != null) {
               aggregateCompletionStage.dependsOn(cacheNotifier.notifyCacheEntryVisited((K) key, value, true, ImmutableContext.INSTANCE, null));
               aggregateCompletionStage.dependsOn(cacheNotifier.notifyCacheEntryVisited((K) key, value, false, ImmutableContext.INSTANCE, null));
            }
            map.put(entry.getKey(), entry);
         }
      }
      if (aggregateCompletionStage != null) {
         CompletionStages.join(aggregateCompletionStage.freeze());
      }
      return map;
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return CompletableFuture.completedFuture(compute(key, remappingFunction, metadata));
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return CompletableFuture.completedFuture(computeIfPresent(key, remappingFunction, metadata));
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata) {
      return CompletableFuture.completedFuture(computeIfAbsent(key, mappingFunction, metadata));
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return CompletableFuture.completedFuture(merge(key, value, remappingFunction, lifespan, lifespanUnit));
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return CompletableFuture.completedFuture(merge(key, value, remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return CompletableFuture.completedFuture(merge(key, value, remappingFunction, metadata));
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return CompletableFuture.completedFuture(compute(key, remappingFunction));
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return CompletableFuture.completedFuture(compute(key, remappingFunction, lifespan, lifespanUnit));
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return CompletableFuture.completedFuture(compute(key, remappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit));
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction) {
      return CompletableFuture.completedFuture(computeIfAbsent(key, mappingFunction));
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return CompletableFuture.completedFuture(computeIfAbsent(key, mappingFunction, lifespan, lifespanUnit));
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return CompletableFuture.completedFuture(computeIfAbsent(key, mappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit));
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return CompletableFuture.completedFuture(computeIfPresent(key, remappingFunction));
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return CompletableFuture.completedFuture(computeIfPresent(key, remappingFunction, lifespan, lifespanUnit));
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return CompletableFuture.completedFuture(computeIfPresent(key, remappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit));
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return CompletableFuture.completedFuture(merge(key, value, remappingFunction));
   }

   @Override
   public Map<K, V> getGroup(String groupName) {
      return Collections.emptyMap();
   }

   @Override
   public void removeGroup(String groupName) {
   }

   @Override
   public AvailabilityMode getAvailability() {
      return AvailabilityMode.AVAILABLE;
   }

   @Override
   public void setAvailability(AvailabilityMode availabilityMode) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Boolean> touch(Object key, boolean touchEvenIfExpired) {
      return touch(key, -1, touchEvenIfExpired);
   }

   @Override
   public CompletionStage<Boolean> touch(Object key, int segment, boolean touchEvenIfExpired) {
      Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
      if (segment < 0) {
         segment = keyPartitioner.getSegment(key);
      }
      InternalCacheEntry<K, V> entry = dataContainer.peek(segment, key);
      if (entry != null) {
         long currentTime = timeService.wallClockTime();
         if (touchEvenIfExpired || !entry.isExpired(currentTime)) {
            return CompletableFutures.booleanStage(dataContainer.touch(segment, key, currentTime));
         }
      }
      return CompletableFutures.completedFalse();
   }

   @Override
   public void evict(K key) {
      ByRef<InternalCacheEntry<K, V>> oldEntryRef = new ByRef<>(null);
      getDataContainer().compute(key, (k, oldEntry, factory) -> {
         if (!isNull(oldEntry)) {
            oldEntryRef.set(oldEntry);
         }
         return null;
      });
      InternalCacheEntry<K, V> oldEntry = oldEntryRef.get();
      if (hasListeners && oldEntry != null) {
         CompletionStages.join(cacheNotifier.notifyCacheEntriesEvicted(Collections.singleton(oldEntry), ImmutableContext.INSTANCE, null));
      }
   }

   @Override
   public Configuration getCacheConfiguration() {
      return configuration;
   }

   @Override
   public EmbeddedCacheManager getCacheManager() {
      return cacheManager;
   }

   @Override
   public AdvancedCache<K, V> getAdvancedCache() {
      return this;
   }

   @Override
   public ComponentStatus getStatus() {
      return componentRegistry.getStatus();
   }

   @ManagedAttribute(
         description = "Returns the cache status",
         displayName = "Cache status",
         dataType = DataType.TRAIT
   )
   public String getCacheStatus() {
      return getStatus().toString();
   }

   protected boolean checkExpiration(InternalCacheEntry<K, V> entry, long now) {
      if (entry.isExpired(now)) {
         // we have to check the expiration under lock
         return null == dataContainer.compute(entry.getKey(), (key, oldEntry, factory) -> {
            if (entry.isExpired(now)) {
               CompletionStages.join(cacheNotifier.notifyCacheEntryExpired(key, oldEntry.getValue(),
                     oldEntry.getMetadata(), ImmutableContext.INSTANCE));
               return null;
            }
            return oldEntry;
         });
      }
      return false;
   }

   @Override
   public int size() {
      // we have to iterate in order to provide precise result in case of expiration
      long now = Long.MIN_VALUE;
      int size = 0;
      DataContainer<K, V> dataContainer = getDataContainer();
      for (InternalCacheEntry<K, V> entry : dataContainer) {
         if (entry.canExpire()) {
            if (now == Long.MIN_VALUE) now = timeService.wallClockTime();
            if (!checkExpiration(entry, now)) {
               ++size;
               if (size < 0) {
                  return Integer.MAX_VALUE;
               }
            }
         } else {
            ++size;
            if (size < 0) {
               return Integer.MAX_VALUE;
            }
         }
      }
      return size;
   }

   @Override
   public CompletableFuture<Long> sizeAsync() {
      // we have to iterate in order to provide precise result in case of expiration
      long now = Long.MIN_VALUE;
      long size = 0;
      DataContainer<K, V> dataContainer = getDataContainer();
      for (InternalCacheEntry<K, V> entry : dataContainer) {
         if (entry.canExpire()) {
            if (now == Long.MIN_VALUE) now = timeService.wallClockTime();
            if (!checkExpiration(entry, now)) {
               ++size;
            }
         } else {
            ++size;
         }
      }
      return CompletableFuture.completedFuture(size);
   }

   @Override
   public boolean isEmpty() {
      long now = Long.MIN_VALUE;
      DataContainer<K, V> dataContainer = getDataContainer();
      for (InternalCacheEntry<K, V> entry : dataContainer) {
         if (entry.canExpire()) {
            if (now == Long.MIN_VALUE) now = timeService.wallClockTime();
            if (!checkExpiration(entry, now)) {
               return false;
            }
         } else {
            return false;
         }
      }
      return true;
   }

   @Override
   public boolean containsKey(Object key) {
      return get(key) != null;
   }

   @Override
   public boolean containsValue(Object value) {
      Objects.requireNonNull(value, NULL_VALUES_NOT_SUPPORTED);
      for (InternalCacheEntry<K, V> ice : getDataContainer()) {
         if (Objects.equals(ice.getValue(), value)) return true;
      }
      return false;
   }

   @Override
   public V get(Object key) {
      Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
      InternalCacheEntry<K, V> entry = internalGet(key);
      if (entry == null) {
         return null;
      } else {
         if (hasListeners) {
            CompletionStages.join(cacheNotifier.notifyCacheEntryVisited(entry.getKey(), entry.getValue(), true, ImmutableContext.INSTANCE, null));
            CompletionStages.join(cacheNotifier.notifyCacheEntryVisited(entry.getKey(), entry.getValue(), false, ImmutableContext.INSTANCE, null));
         }
         return entry.getValue();
      }
   }

   @Override
   public CacheSet<K> keySet() {
      return new KeySet();
   }

   @Override
   public CacheCollection<V> values() {
      return new Values();
   }

   @Override
   public CacheSet<Entry<K, V>> entrySet() {
      return new EntrySet();
   }

   @Override
   public CacheSet<CacheEntry<K, V>> cacheEntrySet() {
      return new CacheEntrySet();
   }

   @Override
   public LockedStream<K, V> lockedStream() {
      throw new UnsupportedOperationException("Simple cache doesn't support lock stream!");
   }

   @Override
   public CompletableFuture<Boolean> removeLifespanExpired(K key, V value, Long lifespan) {
      checkExpiration(getDataContainer().peek(key), timeService.wallClockTime());
      return CompletableFutures.completedTrue();
   }

   @Override
   public CompletableFuture<Boolean> removeMaxIdleExpired(K key, V value) {
      if (checkExpiration(getDataContainer().peek(key), timeService.wallClockTime())) {
         return CompletableFutures.completedTrue();
      }
      return CompletableFutures.completedFalse();
   }

   @Override
   public AdvancedCache<?, ?> withEncoding(Class<? extends Encoder> encoder) {
      throw new UnsupportedOperationException();
   }

   @Override
   public AdvancedCache<?, ?> withEncoding(Class<? extends Encoder> keyEncoder, Class<? extends Encoder> valueEncoder) {
      throw new UnsupportedOperationException();
   }

   @Override
   public AdvancedCache<Object, Object> withKeyEncoding(Class<? extends Encoder> encoder) {
      throw new UnsupportedOperationException();
   }

   @Override
   public AdvancedCache<?, ?> withMediaType(String keyMediaType, String valueMediaType) {
      throw new UnsupportedOperationException();
   }

   @Override
   public <K1, V1> AdvancedCache<K1, V1> withMediaType(MediaType keyMediaType, MediaType valueMediaType) {
      throw new UnsupportedOperationException();
   }

   @Override
   public AdvancedCache<K, V> withStorageMediaType() {
      throw new UnsupportedOperationException();
   }

   @Override
   public DataConversion getKeyDataConversion() {
      throw new UnsupportedOperationException("Conversion requires EncoderCache");
   }

   @Override
   public DataConversion getValueDataConversion() {
      throw new UnsupportedOperationException("Conversion requires EncoderCache");
   }

   @Override
   @ManagedOperation(
         description = "Clears the cache",
         displayName = "Clears the cache", name = "clear"
   )
   public void clear() {
      DataContainer<K, V> dataContainer = getDataContainer();
      boolean hasListeners = this.hasListeners;
      ArrayList<InternalCacheEntry<K, V>> copyEntries;
      if (hasListeners && cacheNotifier.hasListener(CacheEntryRemoved.class)) {
         copyEntries = new ArrayList<>(dataContainer.sizeIncludingExpired());
         dataContainer.forEach(entry -> {
            copyEntries.add(entry);
            CompletionStages.join(cacheNotifier.notifyCacheEntryRemoved(entry.getKey(), entry.getValue(), entry.getMetadata(), true, ImmutableContext.INSTANCE, null));
         });
      } else {
         copyEntries = null;
      }
      dataContainer.clear();
      if (copyEntries != null) {
         for (InternalCacheEntry<K, V> entry : copyEntries) {
            CompletionStages.join(cacheNotifier.notifyCacheEntryRemoved(entry.getKey(), entry.getValue(), entry.getMetadata(), false, ImmutableContext.INSTANCE, null));
         }
      }
   }

   @Override
   public String getName() {
      return name;
   }

   @ManagedAttribute(
         description = "Returns the cache name",
         displayName = "Cache name",
         dataType = DataType.TRAIT
   )
   public String getCacheName() {
      return getName() + "(" + getCacheConfiguration().clustering().cacheMode().toString().toLowerCase() + ")";
   }

   @Override
   @ManagedAttribute(
         description = "Returns the version of Infinispan",
         displayName = "Infinispan version",
         dataType = DataType.TRAIT
   )
   public String getVersion() {
      return Version.getVersion();
   }

   @ManagedAttribute(
         description = "Returns the cache configuration in form of properties",
         displayName = "Cache configuration properties",
         dataType = DataType.TRAIT
   )
   public Properties getConfigurationAsProperties() {
      return new PropertyFormatter().format(configuration);
   }

   @Override
   public V put(K key, V value) {
      return getAndPutInternal(key, value, defaultMetadata);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      Metadata metadata = createMetadata(lifespan, unit);
      return getAndPutInternal(key, value, metadata);
   }

   protected V getAndPutInternal(K key, V value, Metadata metadata) {
      CacheEntry<K, V> oldEntry = getAndPutInternalEntry(key, value, metadata);
      return oldEntry != null ? oldEntry.getValue() : null;
   }

   private CacheEntry<K, V> getAndPutInternalEntry(K key, V value, Metadata metadata) {
      Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
      Objects.requireNonNull(value, NULL_VALUES_NOT_SUPPORTED);
      ByRef<CacheEntry<K, V>> oldEntryRef = new ByRef<>(null);
      boolean hasListeners = this.hasListeners;
      getDataContainer().compute(key, (k, oldEntry, factory) -> {
         if (isNull(oldEntry)) {
            if (hasListeners) {
               CompletionStages.join(cacheNotifier.notifyCacheEntryCreated(key, value, metadata, true, ImmutableContext.INSTANCE, null));
            }
         } else {
            // Have to clone because the value can be updated.
            oldEntryRef.set(oldEntry.clone());
            if (hasListeners) {
               CompletionStages.join(cacheNotifier.notifyCacheEntryModified(key, value, metadata, oldEntry.getValue(), oldEntry.getMetadata(), true, ImmutableContext.INSTANCE, null));
            }
         }
         if (oldEntry == null) {
            return factory.create(k, value, metadata);
         } else {
            return factory.update(oldEntry, value, metadata);
         }
      });
      CacheEntry<K, V> oldEntry = oldEntryRef.get();
      if (hasListeners) {
         V oldValue = oldEntry != null ? oldEntry.getValue() : null;
         if (oldValue == null) {
            CompletionStages.join(cacheNotifier.notifyCacheEntryCreated(key, value, metadata, false, ImmutableContext.INSTANCE, null));
         } else {
            CompletionStages.join(cacheNotifier.notifyCacheEntryModified(key, value, metadata, oldValue, oldEntry.getMetadata(), false, ImmutableContext.INSTANCE, null));
         }
      }
      return oldEntry;
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      Metadata metadata = createMetadata(lifespan, unit);
      return putIfAbsentInternal(key, value, metadata);
   }

   @Override
   public V putIfAbsent(K key, V value, Metadata metadata) {
      return putIfAbsentInternal(key, value, applyDefaultMetadata(metadata));
   }

   protected V putIfAbsentInternal(K key, V value, Metadata metadata) {
      CacheEntry<K, V> entry = putIfAbsentInternalEntry(key, value, metadata);
      return entry != null ? entry.getValue() : null;
   }

   private CacheEntry<K, V> putIfAbsentInternalEntry(K key, V value, Metadata metadata) {
      Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
      Objects.requireNonNull(value, NULL_VALUES_NOT_SUPPORTED);
      ByRef<CacheEntry<K, V>> previousEntryRef = new ByRef<>(null);
      boolean hasListeners = this.hasListeners;
      getDataContainer().compute(key, (k, oldEntry, factory) -> {
         if (isNull(oldEntry)) {
            if (hasListeners) {
               CompletionStages.join(cacheNotifier.notifyCacheEntryCreated(key, value, metadata, true, ImmutableContext.INSTANCE, null));
            }
            return factory.create(k, value, metadata);
         } else {
            // Have to clone because the value can be updated.
            previousEntryRef.set(oldEntry.clone());
            return oldEntry;
         }
      });
      CacheEntry<K, V> previousEntry = previousEntryRef.get();
      if (hasListeners && previousEntry == null) {
         CompletionStages.join(cacheNotifier.notifyCacheEntryCreated(key, value, metadata, false, ImmutableContext.INSTANCE, null));
      }
      return previousEntry;
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      putAllInternal(map, createMetadata(lifespan, unit));
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
      Objects.requireNonNull(value, NULL_VALUES_NOT_SUPPORTED);
      Metadata metadata = createMetadata(lifespan, unit);
      return getAndReplaceInternal(key, value, metadata);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      return replaceInternal(key, oldValue, value, createMetadata(lifespan, unit));
   }

   protected V getAndReplaceInternal(K key, V value, Metadata metadata) {
      CacheEntry<K, V> oldEntry = getAndReplaceInternalEntry(key, value, metadata);
      return oldEntry != null ? oldEntry.getValue() : null;
   }

   private CacheEntry<K, V> getAndReplaceInternalEntry(K key, V value, Metadata metadata) {
      Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
      Objects.requireNonNull(value, NULL_VALUES_NOT_SUPPORTED);
      ByRef<CacheEntry<K, V>> ref = new ByRef<>(null);
      boolean hasListeners = this.hasListeners;
      getDataContainer().compute(key, (k, oldEntry, factory) -> {
         if (!isNull(oldEntry)) {
            if (hasListeners) {
               CompletionStages.join(cacheNotifier.notifyCacheEntryModified(key, value, metadata, oldEntry.getValue(), oldEntry.getMetadata(), true, ImmutableContext.INSTANCE, null));
            }
            // Have to clone because the value can be updated.
            ref.set(oldEntry.clone());
            return factory.update(oldEntry, value, metadata);
         } else {
            return oldEntry;
         }
      });
      CacheEntry<K, V> oldRef = ref.get();
      if (hasListeners && oldRef != null && oldRef.getValue() != null) {
         V oldValue = oldRef.getValue();
         CompletionStages.join(cacheNotifier.notifyCacheEntryModified(key, value, metadata, oldValue, oldRef.getMetadata(), false, ImmutableContext.INSTANCE, null));
      }
      return oldRef;
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Metadata metadata = createMetadata(lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return getAndPutInternal(key, value, metadata);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Metadata metadata = createMetadata(lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return putIfAbsentInternal(key, value, metadata);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      putAllInternal(map, createMetadata(lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Metadata metadata = createMetadata(lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return getAndReplaceInternal(key, value, metadata);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Metadata metadata = createMetadata(lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return replaceInternal(key, oldValue, value, metadata);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, Metadata metadata) {
      return replaceInternal(key, oldValue, value, applyDefaultMetadata(metadata));
   }

   protected boolean replaceInternal(K key, V oldValue, V value, Metadata metadata) {
      Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
      Objects.requireNonNull(value, NULL_VALUES_NOT_SUPPORTED);
      Objects.requireNonNull(oldValue, NULL_VALUES_NOT_SUPPORTED);
      ValueAndMetadata<V> oldRef = new ValueAndMetadata<>();
      boolean hasListeners = this.hasListeners;
      getDataContainer().compute(key, (k, oldEntry, factory) -> {
         V prevValue = getValue(oldEntry);
         if (Objects.equals(prevValue, oldValue)) {
            oldRef.set(prevValue, oldEntry.getMetadata());
            if (hasListeners) {
               CompletionStages.join(cacheNotifier.notifyCacheEntryModified(key, value, metadata, prevValue, oldEntry.getMetadata(), true, ImmutableContext.INSTANCE, null));
            }
            return factory.update(oldEntry, value, metadata);
         } else {
            return oldEntry;
         }
      });
      if (oldRef.getValue() != null) {
         if (hasListeners) {
            CompletionStages.join(cacheNotifier.notifyCacheEntryModified(key, value, metadata, oldRef.getValue(), oldRef.getMetadata(), false, ImmutableContext.INSTANCE, null));
         }
         return true;
      } else {
         return false;
      }
   }

   @Override
   public V remove(Object key) {
      CacheEntry<K, V> oldEntry = removeEntry(key);
      return oldEntry != null ? oldEntry.getValue() : null;
   }

   @Override
   public <T> Query<T> query(String query) {
      throw log.querySimpleCacheNotSupported();
   }

   @Override
   public ContinuousQuery<K, V> continuousQuery() {
      throw log.querySimpleCacheNotSupported();
   }

   private CacheEntry<K, V> removeEntry(Object key) {
      Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
      ByRef<InternalCacheEntry<K, V>> oldEntryRef = new ByRef<>(null);
      boolean hasListeners = this.hasListeners;
      getDataContainer().compute((K) key, (k, oldEntry, factory) -> {
         if (!isNull(oldEntry)) {
            if (hasListeners) {
               CompletionStages.join(cacheNotifier.notifyCacheEntryRemoved(oldEntry.getKey(), oldEntry.getValue(), oldEntry.getMetadata(), true, ImmutableContext.INSTANCE, null));
            }
            oldEntryRef.set(oldEntry);
         }
         return null;
      });
      InternalCacheEntry<K, V> oldEntry = oldEntryRef.get();
      if (oldEntry != null && hasListeners) {
         CompletionStages.join(cacheNotifier.notifyCacheEntryRemoved(oldEntry.getKey(), oldEntry.getValue(), oldEntry.getMetadata(), false, ImmutableContext.INSTANCE, null));
      }

      return oldEntry;
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map) {
      putAllInternal(map, defaultMetadata);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value) {
      return CompletableFuture.completedFuture(put(key, value));
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return CompletableFuture.completedFuture(put(key, value, lifespan, unit));
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return CompletableFuture.completedFuture(put(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit));
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      putAll(data);
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      putAll(data, lifespan, unit);
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      putAll(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> map, Metadata metadata) {
      putAll(map, metadata);
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletableFuture<Void> clearAsync() {
      clear();
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value) {
      return CompletableFuture.completedFuture(putIfAbsent(key, value));
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      return CompletableFuture.completedFuture(putIfAbsent(key, value, lifespan, unit));
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return CompletableFuture.completedFuture(putIfAbsent(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit));
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, Metadata metadata) {
      return CompletableFuture.completedFuture(putIfAbsent(key, value, metadata));
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> putIfAbsentAsyncEntry(K key, V value, Metadata metadata) {
      return CompletableFuture.completedFuture(putIfAbsentInternalEntry(key, value, metadata));
   }

   @Override
   public CompletableFuture<V> removeAsync(Object key) {
      return CompletableFuture.completedFuture(remove(key));
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> removeAsyncEntry(Object key) {
      return CompletableFuture.completedFuture(removeEntry(key));
   }

   @Override
   public CompletableFuture<Boolean> removeAsync(Object key, Object value) {
      return CompletableFuture.completedFuture(remove(key, value));
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value) {
      return CompletableFuture.completedFuture(replace(key, value));
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return CompletableFuture.completedFuture(replace(key, value, lifespan, unit));
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return CompletableFuture.completedFuture(replace(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit));
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return CompletableFuture.completedFuture(replace(key, oldValue, newValue));
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return CompletableFuture.completedFuture(replace(key, oldValue, newValue, lifespan, unit));
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return CompletableFuture.completedFuture(replace(key, oldValue, newValue, lifespan, lifespanUnit, maxIdle, maxIdleUnit));
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, Metadata metadata) {
      return CompletableFuture.completedFuture(replace(key, value, metadata));
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> replaceAsyncEntry(K key, V value, Metadata metadata) {
      return CompletableFuture.completedFuture(getAndReplaceInternalEntry(key, value, metadata));
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, Metadata metadata) {
      return CompletableFuture.completedFuture(replace(key, oldValue, newValue, metadata));
   }

   @Override
   public CompletableFuture<V> getAsync(K key) {
      InternalCacheEntry<K, V> e = getDataContainer().peek(key);
      if (e == null) return CompletableFutures.completedNull();
      if (!e.canExpire())
         return CompletableFuture.completedFuture(get(key));

      long currentTimeMs = timeService.wallClockTime();
      if (!e.isExpired(currentTimeMs))
         return invokeGetListener(e).thenApply(ignore -> e.getValue()).toCompletableFuture();

      InternalExpirationManager<K, V> iem = (InternalExpirationManager<K, V>) getExpirationManager();
      return iem.entryExpiredInMemory(e, currentTimeMs, false)
            .thenApply(expired -> {
               if (expired) return null;
               return e.getValue();
            })
            .thenCompose(v -> v != null
                  ? invokeGetListener(e).thenApply(ignore -> v)
                  : CompletableFutures.completedNull());
   }

   private CompletionStage<Void> invokeGetListener(InternalCacheEntry<K, V> e) {
      if (!hasListeners) return CompletableFutures.completedNull();

      return cacheNotifier.notifyCacheEntryVisited(e.getKey(), e.getValue(), true, ImmutableContext.INSTANCE, null)
            .thenCompose(ignore ->
                  cacheNotifier.notifyCacheEntryVisited(e.getKey(), e.getValue(), false, ImmutableContext.INSTANCE, null));
   }

   @Override
   public boolean startBatch() {
      // invocation batching implies CacheImpl
      throw CONFIG.invocationBatchingNotEnabled();
   }

   @Override
   public void endBatch(boolean successful) {
      // invocation batching implies CacheImpl
      throw CONFIG.invocationBatchingNotEnabled();
   }

   @Override
   public V putIfAbsent(K key, V value) {
      return putIfAbsentInternal(key, value, defaultMetadata);
   }

   @Override
   public boolean remove(Object key, Object value) {
      Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
      Objects.requireNonNull(value, NULL_VALUES_NOT_SUPPORTED);
      ByRef<InternalCacheEntry<K, V>> oldEntryRef = new ByRef<>(null);
      boolean hasListeners = this.hasListeners;
      getDataContainer().compute((K) key, (k, oldEntry, factory) -> {
         V oldValue = getValue(oldEntry);
         if (Objects.equals(oldValue, value)) {
            if (hasListeners) {
               CompletionStages.join(cacheNotifier.notifyCacheEntryRemoved(oldEntry.getKey(), oldValue, oldEntry.getMetadata(), true, ImmutableContext.INSTANCE, null));
            }
            oldEntryRef.set(oldEntry);
            return null;
         } else {
            return oldEntry;
         }
      });
      InternalCacheEntry<K, V> oldEntry = oldEntryRef.get();
      if (oldEntry != null) {
         if (hasListeners) {
            CompletionStages.join(cacheNotifier.notifyCacheEntryRemoved(oldEntry.getKey(), oldEntry.getValue(), oldEntry.getMetadata(), false, ImmutableContext.INSTANCE, null));
         }
         return true;
      } else {
         return false;
      }
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      return replaceInternal(key, oldValue, newValue, defaultMetadata);
   }

   @Override
   public V replace(K key, V value) {
      return getAndReplaceInternal(key, value, defaultMetadata);
   }

   @Override
   public <C> CompletionStage<Void> addListenerAsync(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter) {
      if (!hasListeners && canFire(listener)) {
         hasListeners = true;
      }
      return cacheNotifier.addListenerAsync(listener, filter, converter);
   }

   @Override
   public CompletionStage<Void> addListenerAsync(Object listener) {
      if (!hasListeners && canFire(listener)) {
         hasListeners = true;
      }
      return cacheNotifier.addListenerAsync(listener);
   }

   @Override
   public CompletionStage<Void> removeListenerAsync(Object listener) {
      return cacheNotifier.removeListenerAsync(listener);
   }

   @Override
   public <C> CompletionStage<Void> addFilteredListenerAsync(Object listener,
                                       CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter,
                                       Set<Class<? extends Annotation>> filterAnnotations) {
      if (!hasListeners && canFire(listener)) {
         hasListeners = true;
      }
      return cacheNotifier.addFilteredListenerAsync(listener, filter, converter, filterAnnotations);
   }

   @Override
   public <C> CompletionStage<Void> addStorageFormatFilteredListenerAsync(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter, Set<Class<? extends Annotation>> filterAnnotations) {
      throw new UnsupportedOperationException();
   }

   private boolean canFire(Object listener) {
      for (Method m : listener.getClass().getMethods()) {
         for (Class<? extends Annotation> annotation : FIRED_EVENTS) {
            if (m.isAnnotationPresent(annotation)) {
               return true;
            }
         }
      }
      return false;
   }

   private Metadata applyDefaultMetadata(Metadata metadata) {
      Metadata.Builder builder = metadata.builder();
      return builder != null ? builder.merge(defaultMetadata).build() : metadata;
   }

   private Metadata createMetadata(long lifespan, TimeUnit unit) {
      return new EmbeddedMetadata.Builder().lifespan(lifespan, unit).maxIdle(configuration.expiration().maxIdle()).build();
   }

   private Metadata createMetadata(long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new EmbeddedMetadata.Builder()
            .lifespan(lifespan, lifespanUnit)
            .maxIdle(maxIdleTime, maxIdleTimeUnit)
            .build();
   }

   @Override
   public AdvancedCache<K, V> withFlags(Flag... flags) {
      // the flags are mostly ignored
      return this;
   }

   @Override
   public AdvancedCache<K, V> withFlags(Collection<Flag> flags) {
      return this;
   }

   @Override
   public AdvancedCache<K, V> noFlags() {
      return this;
   }

   @Override
   public AdvancedCache<K, V> transform(Function<AdvancedCache<K, V>, ? extends AdvancedCache<K, V>> transformation) {
      return transformation.apply(this);
   }

   @Override
   public AdvancedCache<K, V> withSubject(Subject subject) {
      return this; // NO-OP
   }

   @Override
   public ExpirationManager<K, V> getExpirationManager() {
      return getComponentRegistry().getComponent(InternalExpirationManager.class);
   }

   @Override
   public ComponentRegistry getComponentRegistry() {
      return componentRegistry;
   }

   @Override
   public DistributionManager getDistributionManager() {
      return getComponentRegistry().getComponent(DistributionManager.class);
   }

   @Override
   public AuthorizationManager getAuthorizationManager() {
      return getComponentRegistry().getComponent(AuthorizationManager.class);
   }

   @Override
   public AdvancedCache<K, V> lockAs(Object lockOwner) {
      throw new UnsupportedOperationException("lockAs method not supported with Simple Cache!");
   }

   @Override
   public boolean lock(K... keys) {
      throw CONTAINER.lockOperationsNotSupported();
   }

   @Override
   public boolean lock(Collection<? extends K> keys) {
      throw CONTAINER.lockOperationsNotSupported();
   }

   @Override
   public RpcManager getRpcManager() {
      return null;
   }

   @Override
   public BatchContainer getBatchContainer() {
      return null;
   }

   @Override
   public DataContainer<K, V> getDataContainer() {
      DataContainer<K, V> dataContainer = this.dataContainer;
      if (dataContainer == null) {
         ComponentStatus status = getStatus();
         switch (status) {
            case STOPPING:
               throw CONTAINER.cacheIsStopping(name);
            case TERMINATED:
            case FAILED:
               throw CONTAINER.cacheIsTerminated(name, status.toString());
            default:
               throw new IllegalStateException("Status: " + status);
         }
      }
      return dataContainer;
   }

   @Override
   public TransactionManager getTransactionManager() {
      return null;
   }

   @Override
   public LockManager getLockManager() {
      return null;
   }

   @Override
   public Stats getStats() {
      return null;
   }

   @Override
   public XAResource getXAResource() {
      return null;
   }

   @Override
   public ClassLoader getClassLoader() {
      return null;
   }

   @Override
   public V put(K key, V value, Metadata metadata) {
      return getAndPutInternal(key, value, applyDefaultMetadata(metadata));
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, Metadata metadata) {
      putAllInternal(map, applyDefaultMetadata(metadata));
   }

   protected void putAllInternal(Map<? extends K, ? extends V> map, Metadata metadata) {
      for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
         Objects.requireNonNull(entry.getKey(), NULL_KEYS_NOT_SUPPORTED);
         Objects.requireNonNull(entry.getValue(), NULL_VALUES_NOT_SUPPORTED);
      }
      for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
         getAndPutInternal(entry.getKey(), entry.getValue(), metadata);
      }
   }

   @Override
   public V replace(K key, V value, Metadata metadata) {
      return getAndReplaceInternal(key, value, applyDefaultMetadata(metadata));
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
      ByRef<V> newValueRef = new ByRef<>(null);
      return computeIfAbsentInternal(key, mappingFunction, newValueRef, defaultMetadata);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata) {
      ByRef<V> newValueRef = new ByRef<>(null);
      return computeIfAbsentInternal(key, mappingFunction, newValueRef, metadata);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit) {
      ByRef<V> newValueRef = new ByRef<>(null);
      return computeIfAbsentInternal(key, mappingFunction, newValueRef, createMetadata(lifespan, lifespanUnit));
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      ByRef<V> newValueRef = new ByRef<>(null);
      return computeIfAbsentInternal(key, mappingFunction, newValueRef, createMetadata(lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   protected V computeIfAbsentInternal(K key, Function<? super K, ? extends V> mappingFunction, ByRef<V> newValueRef) {
      return computeIfAbsentInternal(key, mappingFunction, newValueRef, defaultMetadata);
   }

   private V computeIfAbsentInternal(K key, Function<? super K, ? extends V> mappingFunction, ByRef<V> newValueRef, Metadata metadata) {
      Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
      Objects.requireNonNull(mappingFunction, NULL_FUNCTION_NOT_SUPPORTED);
      boolean hasListeners = this.hasListeners;
      componentRegistry.wireDependencies(mappingFunction);
      InternalCacheEntry<K, V> returnEntry = getDataContainer().compute(key, (k, oldEntry, factory) -> {
         V oldValue = getValue(oldEntry);
         if (oldValue == null) {
            V newValue = mappingFunction.apply(k);
            if (newValue == null) {
               return null;
            } else {
               if (hasListeners) {
                  CompletionStages.join(cacheNotifier.notifyCacheEntryCreated(k, newValue, metadata, true, ImmutableContext.INSTANCE, null));
               }
               newValueRef.set(newValue);
               return factory.create(k, newValue, metadata);
            }
         } else {
            return oldEntry;
         }
      });
      V newValue = newValueRef.get();
      if (hasListeners && newValue != null) {
         CompletionStages.join(cacheNotifier.notifyCacheEntryCreated(key, newValueRef.get(), metadata, false, ImmutableContext.INSTANCE, null));
      }
      return returnEntry == null ? null : returnEntry.getValue();
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      CacheEntryChange<K, V> ref = new CacheEntryChange<>();
      return computeIfPresentInternal(key, remappingFunction, ref, defaultMetadata);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      CacheEntryChange<K, V> ref = new CacheEntryChange<>();
      return computeIfPresentInternal(key, remappingFunction, ref, createMetadata(lifespan, lifespanUnit));
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      CacheEntryChange<K, V> ref = new CacheEntryChange<>();
      return computeIfPresentInternal(key, remappingFunction, ref, createMetadata(lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      CacheEntryChange<K, V> ref = new CacheEntryChange<>();
      return computeIfPresentInternal(key, remappingFunction, ref, metadata);
   }

   protected V computeIfPresentInternal(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, CacheEntryChange<K, V> ref) {
      return computeIfPresentInternal(key, remappingFunction, ref, defaultMetadata);
   }

   private V computeIfPresentInternal(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, CacheEntryChange<K, V> ref, Metadata metadata) {
      Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
      Objects.requireNonNull(remappingFunction, NULL_FUNCTION_NOT_SUPPORTED);
      boolean hasListeners = this.hasListeners;
      componentRegistry.wireDependencies(remappingFunction);
      getDataContainer().compute(key, (k, oldEntry, factory) -> {
         V oldValue = getValue(oldEntry);
         if (oldValue != null) {
            V newValue = remappingFunction.apply(k, oldValue);
            if (newValue == null) {
               if (hasListeners) {
                  CompletionStages.join(cacheNotifier.notifyCacheEntryRemoved(k, oldValue, oldEntry.getMetadata(), true, ImmutableContext.INSTANCE, null));
               }
               ref.set(k, null, oldValue, oldEntry.getMetadata());
               return null;
            } else {
               if (hasListeners) {
                  CompletionStages.join(cacheNotifier.notifyCacheEntryModified(k, newValue, metadata, oldValue, oldEntry.getMetadata(), true, ImmutableContext.INSTANCE, null));
               }
               ref.set(k, newValue, oldValue, oldEntry.getMetadata());
               return factory.update(oldEntry, newValue, metadata);
            }
         } else {
            return null;
         }
      });
      V newValue = ref.getNewValue();
      if (hasListeners) {
         if (newValue != null) {
            CompletionStages.join(cacheNotifier.notifyCacheEntryModified(ref.getKey(), newValue, metadata, ref.getOldValue(), ref.getOldMetadata(), false, ImmutableContext.INSTANCE, null));
         } else {
            CompletionStages.join(cacheNotifier.notifyCacheEntryRemoved(ref.getKey(), ref.getOldValue(), ref.getOldMetadata(), false, ImmutableContext.INSTANCE, null));
         }
      }
      return newValue;
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      CacheEntryChange<K, V> ref = new CacheEntryChange<>();
      return computeInternal(key, remappingFunction, ref, defaultMetadata);
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return computeInternal(key, remappingFunction, new CacheEntryChange<>(), createMetadata(lifespan, lifespanUnit));
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return computeInternal(key, remappingFunction, new CacheEntryChange<>(), createMetadata(lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      CacheEntryChange<K, V> ref = new CacheEntryChange<>();
      return computeInternal(key, remappingFunction, ref, metadata);
   }

   protected V computeInternal(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, CacheEntryChange<K, V> ref) {
      return computeInternal(key, remappingFunction, ref, defaultMetadata);
   }

   private V computeInternal(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, CacheEntryChange<K, V> ref, Metadata metadata) {
      Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
      Objects.requireNonNull(remappingFunction, NULL_FUNCTION_NOT_SUPPORTED);
      boolean hasListeners = this.hasListeners;
      componentRegistry.wireDependencies(remappingFunction);
      getDataContainer().compute(key, (k, oldEntry, factory) -> {
         V oldValue = getValue(oldEntry);
         V newValue = remappingFunction.apply(k, oldValue);
         return getUpdatedEntry(k, oldEntry, factory, oldValue, newValue, metadata, ref, hasListeners);
      });
      return notifyAndReturn(ref, hasListeners, metadata);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return mergeInternal(key, value, remappingFunction, new CacheEntryChange<>(), defaultMetadata);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return mergeInternal(key, value, remappingFunction, new CacheEntryChange<>(), createMetadata(lifespan, lifespanUnit));
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return mergeInternal(key, value, remappingFunction, new CacheEntryChange<>(), createMetadata(lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return mergeInternal(key, value, remappingFunction, new CacheEntryChange<>(), metadata);
   }

   protected V mergeInternal(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, CacheEntryChange<K, V> ref, Metadata metadata) {
      Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
      Objects.requireNonNull(value, NULL_VALUES_NOT_SUPPORTED);
      Objects.requireNonNull(remappingFunction, NULL_FUNCTION_NOT_SUPPORTED);
      boolean hasListeners = this.hasListeners;
      getDataContainer().compute(key, (k, oldEntry, factory) -> {
         V oldValue = getValue(oldEntry);
         V newValue = oldValue == null ? value : remappingFunction.apply(oldValue, value);
         return getUpdatedEntry(k, oldEntry, factory, oldValue, newValue, metadata, ref, hasListeners);
      });
      return notifyAndReturn(ref, hasListeners, metadata);
   }

   private V notifyAndReturn(CacheEntryChange<K, V> ref, boolean hasListeners, Metadata metadata) {
      K key = ref.getKey();
      V newValue = ref.getNewValue();
      if (key != null) {
         V oldValue = ref.getOldValue();
         if (hasListeners) {
            if (newValue == null) {
               CompletionStages.join(cacheNotifier.notifyCacheEntryRemoved(key, oldValue, ref.getOldMetadata(), false, ImmutableContext.INSTANCE, null));
            } else if (oldValue == null) {
               CompletionStages.join(cacheNotifier.notifyCacheEntryCreated(key, newValue, metadata, false, ImmutableContext.INSTANCE, null));
            } else {
               CompletionStages.join(cacheNotifier.notifyCacheEntryModified(key, newValue, metadata, oldValue, ref.getOldMetadata(), false, ImmutableContext.INSTANCE, null));
            }
         }
      }
      return newValue;
   }

   private InternalCacheEntry<K, V> getUpdatedEntry(K k, InternalCacheEntry<K, V> oldEntry, InternalEntryFactory factory, V oldValue, V newValue, Metadata metadata, CacheEntryChange<K, V> ref, boolean hasListeners) {
      if (newValue == null) {
         if (oldValue != null) {
            if (hasListeners) {
               CompletionStages.join(cacheNotifier.notifyCacheEntryRemoved(k, oldValue, oldEntry.getMetadata(), true, ImmutableContext.INSTANCE, null));
            }
            ref.set(k, null, oldValue, oldEntry.getMetadata());
         }
         return null;
      } else if (oldValue == null) {
         if (hasListeners) {
            CompletionStages.join(cacheNotifier.notifyCacheEntryCreated(k, newValue, metadata, true, ImmutableContext.INSTANCE, null));
         }
         ref.set(k, newValue, null, null);
         return factory.create(k, newValue, metadata);
      } else {
         if (hasListeners) {
            CompletionStages.join(cacheNotifier.notifyCacheEntryModified(k, newValue, metadata, oldValue, oldEntry.getMetadata(), true, ImmutableContext.INSTANCE, null));
         }
         ref.set(k, newValue, oldValue, oldEntry.getMetadata());
         return factory.update(oldEntry, newValue, metadata);
      }
   }

   // This method can be called only from dataContainer.compute()'s action;
   // as we'll replace the old value when it's expired
   private boolean isNull(InternalCacheEntry<K, V> entry) {
      if (entry == null) {
         return true;
      } else if (entry.canExpire()) {
         if (entry.isExpired(timeService.wallClockTime())) {
            if (cacheNotifier.hasListener(CacheEntryExpired.class)) {
               CompletionStages.join(cacheNotifier.notifyCacheEntryExpired(entry.getKey(), entry.getValue(),
                     entry.getMetadata(), ImmutableContext.INSTANCE));
            }
            return true;
         }
      }
      return false;
   }

   // This method can be called only from dataContainer.compute()'s action!
   private V getValue(InternalCacheEntry<K, V> entry) {
      return isNull(entry) ? null : entry.getValue();
   }

   @Override
   public void forEach(BiConsumer<? super K, ? super V> action) {
      for (Iterator<InternalCacheEntry<K, V>> it = dataContainer.iterator(); it.hasNext(); ) {
         InternalCacheEntry<K, V> ice = it.next();
         action.accept(ice.getKey(), ice.getValue());
      }
   }

   @Override
   public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {

      AggregateCompletionStage<Void> aggregateCompletionStage;
      if (hasListeners && cacheNotifier.hasListener(CacheEntryModified.class)) {
         aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      } else {
         aggregateCompletionStage = null;
      }
      CacheEntryChange<K, V> ref = new CacheEntryChange<>();
      for (Iterator<InternalCacheEntry<K, V>> it = dataContainer.iterator(); it.hasNext(); ) {
         InternalCacheEntry<K, V> ice = it.next();
         getDataContainer().compute(ice.getKey(), (k, oldEntry, factory) -> {
            V oldValue = getValue(oldEntry);
            if (oldValue != null) {
               V newValue = function.apply(k, oldValue);
               Objects.requireNonNull(newValue, NULL_VALUES_NOT_SUPPORTED);
               if (aggregateCompletionStage != null) {
                  aggregateCompletionStage.dependsOn(cacheNotifier.notifyCacheEntryModified(k, newValue, defaultMetadata, oldValue, oldEntry.getMetadata(),
                        true, ImmutableContext.INSTANCE, null));
               }
               ref.set(k, newValue, oldValue, oldEntry.getMetadata());
               return factory.update(oldEntry, newValue, defaultMetadata);
            } else {
               return null;
            }
         });
         if (aggregateCompletionStage != null) {
            aggregateCompletionStage.dependsOn(cacheNotifier.notifyCacheEntryModified(ref.getKey(), ref.getNewValue(), defaultMetadata, ref.getOldValue(),
                  ref.getOldMetadata(), false, ImmutableContext.INSTANCE, null));
         }
      }
      if (aggregateCompletionStage != null) {
         CompletionStages.join(aggregateCompletionStage.freeze());
      }
   }

   protected static class ValueAndMetadata<V> {
      private V value;
      private Metadata metadata;

      public void set(V value, Metadata metadata) {
         this.value = value;
         this.metadata = metadata;
      }

      public V getValue() {
         return value;
      }

      public Metadata getMetadata() {
         return metadata;
      }
   }

   protected static class CacheEntryChange<K, V> {
      private K key;
      private V newValue;
      private V oldValue;
      private Metadata oldMetadata;

      public void set(K key, V newValue, V oldValue, Metadata oldMetadata) {
         this.key = key;
         this.newValue = newValue;
         this.oldValue = oldValue;
         this.oldMetadata = oldMetadata;
      }

      public K getKey() {
         return key;
      }

      public V getNewValue() {
         return newValue;
      }

      public V getOldValue() {
         return oldValue;
      }

      public Metadata getOldMetadata() {
         return oldMetadata;
      }
   }

   protected abstract class EntrySetBase<T extends Entry<K, V>> extends AbstractSet<T> implements CacheSet<T> {
      private final DataContainer<K, V> delegate = getDataContainer();

      @Override
      public int size() {
         return SimpleCacheImpl.this.size();
      }

      @Override
      public boolean isEmpty() {
         return SimpleCacheImpl.this.isEmpty();
      }

      @Override
      public boolean contains(Object o) {
         return delegate.containsKey(o);
      }

      @Override
      public Object[] toArray() {
         return StreamSupport.stream(delegate.spliterator(), false).toArray();
      }

      @Override
      public boolean remove(Object o) {
         if (o instanceof Entry) {
            Entry<K, V> entry = (Entry<K, V>) o;
            return SimpleCacheImpl.this.remove(entry.getKey(), entry.getValue());
         }
         return false;
      }

      @Override
      public boolean retainAll(Collection<?> c) {
         boolean changed = false;
         for (InternalCacheEntry<K, V> entry : getDataContainer()) {
            if (!c.contains(entry)) {
               changed |= SimpleCacheImpl.this.remove(entry.getKey(), entry.getValue());
            }
         }
         return changed;
      }

      @Override
      public boolean removeAll(Collection<?> c) {
         boolean changed = false;
         for (Object o : c) {
            if (o instanceof Entry) {
               Entry<K, V> entry = (Entry<K, V>) o;
               changed |= SimpleCacheImpl.this.remove(entry.getKey(), entry.getValue());
            }
         }
         return changed;
      }

      @Override
      public void clear() {
         SimpleCacheImpl.this.clear();
      }
   }

   protected class EntrySet extends EntrySetBase<Entry<K, V>> implements CacheSet<Entry<K, V>> {
      @Override
      public CloseableIterator<Entry<K, V>> iterator() {
         return Closeables.iterator(new DataContainerRemoveIterator<>(SimpleCacheImpl.this));
      }

      @Override
      public CloseableSpliterator<Entry<K, V>> spliterator() {
         // Cast to raw since we need to convert from ICE to Entry
         return Closeables.spliterator((Spliterator) dataContainer.spliterator());
      }

      @Override
      public boolean add(Entry<K, V> entry) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean addAll(Collection<? extends Entry<K, V>> c) {
         throw new UnsupportedOperationException();
      }

      @Override
      public CacheStream<Entry<K, V>> stream() {
         return cacheStreamCast(new LocalCacheStream<>(new EntryStreamSupplier<>(SimpleCacheImpl.this, null,
               getStreamSupplier(false)), false, componentRegistry));
      }

      @Override
      public CacheStream<Entry<K, V>> parallelStream() {
         return cacheStreamCast(new LocalCacheStream<>(new EntryStreamSupplier<>(SimpleCacheImpl.this, null,
               getStreamSupplier(false)), true, componentRegistry));
      }
   }

   // This is a hack to allow the cast to work.  Java doesn't like subtypes in generics
   private static <K, V> CacheStream<Entry<K, V>> cacheStreamCast(CacheStream stream) {
      return stream;
   }

   protected class CacheEntrySet extends EntrySetBase<CacheEntry<K, V>> implements CacheSet<CacheEntry<K, V>> {
      @Override
      public CloseableIterator<CacheEntry<K, V>> iterator() {
         return Closeables.iterator(new DataContainerRemoveIterator<>(SimpleCacheImpl.this));
      }

      @Override
      public CloseableSpliterator<CacheEntry<K, V>> spliterator() {
         // Cast to raw since we need to convert from ICE to CE
         return Closeables.spliterator((Spliterator) dataContainer.spliterator());
      }

      @Override
      public boolean add(CacheEntry<K, V> entry) {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean addAll(Collection<? extends CacheEntry<K, V>> c) {
         throw new UnsupportedOperationException();
      }

      @Override
      public CacheStream<CacheEntry<K, V>> stream() {
         return new LocalCacheStream<>(new EntryStreamSupplier<>(SimpleCacheImpl.this, null, getStreamSupplier(false)),
               false, componentRegistry);
      }

      @Override
      public CacheStream<CacheEntry<K, V>> parallelStream() {
         return new LocalCacheStream<>(new EntryStreamSupplier<>(SimpleCacheImpl.this, null, getStreamSupplier(true)),
               true, componentRegistry);
      }
   }

   protected class Values extends AbstractSet<V> implements CacheCollection<V> {
      @Override
      public boolean retainAll(Collection<?> c) {
         Set<Object> retained = new HashSet<>(c.size());
         retained.addAll(c);
         boolean changed = false;
         for (InternalCacheEntry<K, V> entry : getDataContainer()) {
            if (!retained.contains(entry.getValue())) {
               changed |= SimpleCacheImpl.this.remove(entry.getKey(), entry.getValue());
            }
         }
         return changed;
      }

      @Override
      public boolean removeAll(Collection<?> c) {
         int removeSize = c.size();
         if (removeSize == 0) {
            return false;
         } else if (removeSize == 1) {
            return remove(c.iterator().next());
         }
         Set<Object> removed = new HashSet<>(removeSize);
         removed.addAll(c);
         boolean changed = false;
         for (InternalCacheEntry<K, V> entry : getDataContainer()) {
            if (removed.contains(entry.getValue())) {
               changed |= SimpleCacheImpl.this.remove(entry.getKey(), entry.getValue());
            }
         }
         return changed;
      }

      @Override
      public boolean remove(Object o) {
         for (InternalCacheEntry<K, V> entry : getDataContainer()) {
            if (Objects.equals(entry.getValue(), o)) {
               if (SimpleCacheImpl.this.remove(entry.getKey(), entry.getValue())) {
                  return true;
               }
            }
         }
         return false;
      }

      @Override
      public void clear() {
         SimpleCacheImpl.this.clear();
      }

      @Override
      public CloseableIterator<V> iterator() {
         return Closeables.iterator(new IteratorMapper<>(new DataContainerRemoveIterator<>(SimpleCacheImpl.this), Map.Entry::getValue));
      }

      @Override
      public CloseableSpliterator<V> spliterator() {
         return Closeables.spliterator(new SpliteratorMapper<>(getDataContainer().spliterator(), Map.Entry::getValue));
      }

      @Override
      public int size() {
         return SimpleCacheImpl.this.size();
      }

      @Override
      public boolean isEmpty() {
         return SimpleCacheImpl.this.isEmpty();
      }

      @Override
      public CacheStream<V> stream() {
         LocalCacheStream<CacheEntry<K, V>> lcs = new LocalCacheStream<>(new EntryStreamSupplier<>(SimpleCacheImpl.this,
               null, getStreamSupplier(false)), false, componentRegistry);
         return lcs.map(CacheEntry::getValue);
      }

      @Override
      public CacheStream<V> parallelStream() {
         LocalCacheStream<CacheEntry<K, V>> lcs = new LocalCacheStream<>(new EntryStreamSupplier<>(SimpleCacheImpl.this,
               null, getStreamSupplier(false)), true, componentRegistry);
         return lcs.map(CacheEntry::getValue);
      }
   }

   protected Supplier<Stream<CacheEntry<K, V>>> getStreamSupplier(boolean parallel) {
      // This is raw due to not being able to cast inner ICE to CE
      Spliterator spliterator = dataContainer.spliterator();
      return () -> StreamSupport.stream(spliterator, parallel);
   }

   protected class KeySet extends AbstractSet<K> implements CacheSet<K> {
      @Override
      public boolean retainAll(Collection<?> c) {
         Set<Object> retained = new HashSet<>(c.size());
         retained.addAll(c);
         boolean changed = false;
         for (InternalCacheEntry<K, V> entry : getDataContainer()) {
            if (!retained.contains(entry.getKey())) {
               changed |= SimpleCacheImpl.this.remove(entry.getKey()) != null;
            }
         }
         return changed;
      }

      @Override
      public boolean remove(Object o) {
         return SimpleCacheImpl.this.remove(o) != null;
      }

      @Override
      public boolean removeAll(Collection<?> c) {
         boolean changed = false;
         for (Object key : c) {
            changed |= SimpleCacheImpl.this.remove(key) != null;
         }
         return changed;
      }

      @Override
      public void clear() {
         SimpleCacheImpl.this.clear();
      }

      @Override
      public CloseableIterator<K> iterator() {
         return Closeables.iterator(new IteratorMapper<>(new DataContainerRemoveIterator<>(SimpleCacheImpl.this), Map.Entry::getKey));
      }

      @Override
      public CloseableSpliterator<K> spliterator() {
         return new SpliteratorMapper<>(dataContainer.spliterator(), Map.Entry::getKey);
      }

      @Override
      public int size() {
         return SimpleCacheImpl.this.size();
      }

      @Override
      public boolean isEmpty() {
         return SimpleCacheImpl.this.isEmpty();
      }

      @Override
      public CacheStream<K> stream() {
         return new LocalCacheStream<>(new KeyStreamSupplier<>(SimpleCacheImpl.this, null, super::stream), false,
               componentRegistry);
      }

      @Override
      public CacheStream<K> parallelStream() {
         return new LocalCacheStream<>(new KeyStreamSupplier<>(SimpleCacheImpl.this, null, super::stream), true,
               componentRegistry);
      }
   }

   @Override
   public CachePublisher<K, V> cachePublisher() {
      return new CachePublisherImpl<>(new SimpleClusterPublisherManager());
   }

   // We ignore segments, parallel, invocationContext, explicitFlags and deliveryGuarantee for all methods
   class SimpleClusterPublisherManager implements ClusterPublisherManager<K, V> {
      Flowable<CacheEntry<K, V>> filteredFlowable(Set<K> keysToInclude) {
         Flowable<CacheEntry<K, V>> flowable = Flowable.<CacheEntry<K, V>>fromIterable(() -> (Iterator) dataContainer.iterator());
         if (keysToInclude != null) {
            flowable = flowable.filter(k -> keysToInclude.contains(k.getKey()));
         }
         return flowable;
      }

      @Override
      public <R> CompletionStage<R> keyReduction(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
            InvocationContext invocationContext, long explicitFlags, DeliveryGuarantee deliveryGuarantee,
            Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
            Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         CompletionStage<R> stage = transformer.apply(filteredFlowable(keysToInclude)
            .map(CacheEntry::getKey));
         return finalizer.apply(Flowable.fromCompletionStage(stage));
      }

      @Override
      public <R> CompletionStage<R> entryReduction(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
            InvocationContext invocationContext, long explicitFlags, DeliveryGuarantee deliveryGuarantee, Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer, Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         CompletionStage<R> stage = transformer.apply(filteredFlowable(keysToInclude));
         return finalizer.apply(Flowable.fromCompletionStage(stage));
      }

      @Override
      public <R> SegmentPublisherSupplier<R> keyPublisher(IntSet segments, Set<K> keysToInclude, InvocationContext invocationContext, long explicitFlags, DeliveryGuarantee deliveryGuarantee, int batchSize, Function<? super Publisher<K>, ? extends Publisher<R>> transformer) {
         return new SegmentPublisherSupplier<>() {
            @Override
            public Publisher<R> publisherWithoutSegments() {
               return transformer.apply(filteredFlowable(keysToInclude).map(CacheEntry::getKey));
            }

            @Override
            public Publisher<Notification<R>> publisherWithSegments() {
               return Flowable.concat(
                     Flowable.fromPublisher(publisherWithoutSegments())
                           .map(r -> Notifications.value(r, 0)),
                     Flowable.just(Notifications.segmentComplete(0))
               );
            }
         };
      }

      @Override
      public <R> SegmentPublisherSupplier<R> entryPublisher(IntSet segments, Set<K> keysToInclude, InvocationContext invocationContext, long explicitFlags, DeliveryGuarantee deliveryGuarantee, int batchSize, Function<? super Publisher<CacheEntry<K, V>>, ? extends Publisher<R>> transformer) {
         return new SegmentPublisherSupplier<>() {
            @Override
            public Publisher<R> publisherWithoutSegments() {
               return transformer.apply(filteredFlowable(keysToInclude));
            }

            @Override
            public Publisher<Notification<R>> publisherWithSegments() {
               return Flowable.concat(
                     Flowable.fromPublisher(publisherWithoutSegments())
                           .map(r -> Notifications.value(r, 0)),
                     Flowable.just(Notifications.segmentComplete(0))
               );
            }
         };
      }

      @Override
      public CompletionStage<Long> sizePublisher(IntSet segments, InvocationContext ctx, long flags) {
         return CompletableFuture.completedFuture((long) dataContainer.size());
      }
   }

   @Override
   public String toString() {
      return "SimpleCache '" + getName() + "'@" + Util.hexIdHashCode(getCacheManager());
   }
}
