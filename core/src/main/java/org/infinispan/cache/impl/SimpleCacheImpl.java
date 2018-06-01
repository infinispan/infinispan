package org.infinispan.cache.impl;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.security.auth.Subject;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.LockedStream;
import org.infinispan.Version;
import org.infinispan.atomic.Delta;
import org.infinispan.batch.BatchContainer;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorCollectionAdapter;
import org.infinispan.commons.util.CloseableIteratorSetAdapter;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.commons.util.SpliteratorMapper;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.format.PropertyFormatter;
import org.infinispan.container.DataContainer;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.ImmutableContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.encoding.DataConversion;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.KeyFilter;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.EmptyAsyncInterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.DisplayType;
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
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.stats.Stats;
import org.infinispan.stream.impl.local.EntryStreamSupplier;
import org.infinispan.stream.impl.local.KeyStreamSupplier;
import org.infinispan.stream.impl.local.LocalCacheStream;
import org.infinispan.util.DataContainerRemoveIterator;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Simple local cache without interceptor stack.
 * The cache still implements {@link AdvancedCache} since it is too troublesome to omit that.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@MBean(objectName = CacheImpl.OBJECT_NAME, description = "Component that represents a simplified cache instance.")
public class SimpleCacheImpl<K, V> implements AdvancedCache<K, V> {
   private final static Log log = LogFactory.getLog(SimpleCacheImpl.class);

   private final static String NULL_KEYS_NOT_SUPPORTED = "Null keys are not supported!";
   private final static String NULL_VALUES_NOT_SUPPORTED = "Null values are not supported!";
   private final static String NULL_FUNCTION_NOT_SUPPORTED = "Null functions are not supported!";
   private final static Class<? extends Annotation>[] FIRED_EVENTS = new Class[]{
         CacheEntryCreated.class, CacheEntryRemoved.class, CacheEntryVisited.class,
         CacheEntryModified.class, CacheEntriesEvicted.class, CacheEntryInvalidated.class,
         CacheEntryExpired.class};

   private final String name;
   private DataConversion keyDataConversion;
   private DataConversion valueDataConversion;

   @Inject private ComponentRegistry componentRegistry;
   @Inject private Configuration configuration;
   @Inject private EmbeddedCacheManager cacheManager;
   @Inject private DataContainer<K, V> dataContainer;
   @Inject private CacheNotifier<K, V> cacheNotifier;
   @Inject private TimeService timeService;

   private Metadata defaultMetadata;

   private boolean hasListeners = false;

   public SimpleCacheImpl(String cacheName) {
      this(cacheName, DataConversion.IDENTITY_KEY, DataConversion.IDENTITY_VALUE);
   }

   public SimpleCacheImpl(String cacheName, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      this.name = cacheName;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   @Inject
   public void injectDependencies() {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
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
               cacheNotifier.notifyCacheEntryCreated(k, value, metadata, true, ImmutableContext.INSTANCE, null);
            }
            isCreatedRef.set(true);
            return factory.create(k, value, metadata);
         } else {
            return oldEntry;
         }
      });
      if (hasListeners && isCreatedRef.get()) {
         cacheNotifier.notifyCacheEntryCreated(key, value, metadata, false, ImmutableContext.INSTANCE, null);
      }
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, Metadata metadata) {
      return CompletableFuture.completedFuture(getAndPutInternal(key, value, applyDefaultMetadata(metadata)));
   }

   @Override
   public Map<K, V> getAll(Set<?> keys) {
      Map<K, V> map = CollectionFactory
            .makeMap(CollectionFactory.computeCapacity(keys.size()));
      for (Object k : keys) {
         Objects.requireNonNull(k, NULL_KEYS_NOT_SUPPORTED);
         InternalCacheEntry<K, V> entry = getDataContainer().get(k);
         if (entry != null) {
            K key = entry.getKey();
            V value = entry.getValue();
            if (hasListeners) {
               cacheNotifier.notifyCacheEntryVisited(key, value, true, ImmutableContext.INSTANCE, null);
               cacheNotifier.notifyCacheEntryVisited(key, value, false, ImmutableContext.INSTANCE, null);
            }
            map.put(key, value);
         }
      }
      return map;
   }

   @Override
   public CompletableFuture<Map<K, V>> getAllAsync(Set<?> keys) {
      return CompletableFuture.completedFuture(getAll(keys));
   }

   @Override
   public CacheEntry<K, V> getCacheEntry(Object k) {
      InternalCacheEntry<K, V> entry = getDataContainer().get(k);
      if (entry != null) {
         K key = entry.getKey();
         V value = entry.getValue();
         if (hasListeners) {
            cacheNotifier.notifyCacheEntryVisited(key, value, true, ImmutableContext.INSTANCE, null);
            cacheNotifier.notifyCacheEntryVisited(key, value, false, ImmutableContext.INSTANCE, null);
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
      Map<K, CacheEntry<K, V>> map = CollectionFactory
            .makeMap(CollectionFactory.computeCapacity(keys.size()));
      for (Object key : keys) {
         Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
         InternalCacheEntry<K, V> entry = getDataContainer().get(key);
         if (entry != null) {
            V value = entry.getValue();
            if (hasListeners) {
               cacheNotifier.notifyCacheEntryVisited((K) key, value, true, ImmutableContext.INSTANCE, null);
               cacheNotifier.notifyCacheEntryVisited((K) key, value, false, ImmutableContext.INSTANCE, null);
            }
            map.put(entry.getKey(), entry);
         }
      }
      return map;
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
         cacheNotifier.notifyCacheEntriesEvicted(Collections.singleton(oldEntry), ImmutableContext.INSTANCE, null);
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
         dataType = DataType.TRAIT,
         displayType = DisplayType.SUMMARY
   )
   public String getCacheStatus() {
      return getStatus().toString();
   }

   protected boolean checkExpiration(InternalCacheEntry<K, V> entry, long now) {
      if (entry.isExpired(now)) {
         // we have to check the expiration under lock
         return null == dataContainer.compute(entry.getKey(), (key, oldEntry, factory) -> {
            if (entry.isExpired(now)) {
               cacheNotifier.notifyCacheEntryExpired(key, oldEntry.getValue(), oldEntry.getMetadata(), ImmutableContext.INSTANCE);
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
      for (V v : getDataContainer().values()) {
         if (Objects.equals(v, value)) return true;
      }
      return false;
   }

   @Override
   public V get(Object key) {
      Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
      InternalCacheEntry<K, V> entry = getDataContainer().get(key);
      if (entry == null) {
         return null;
      } else {
         if (hasListeners) {
            cacheNotifier.notifyCacheEntryVisited(entry.getKey(), entry.getValue(), true, ImmutableContext.INSTANCE, null);
            cacheNotifier.notifyCacheEntryVisited(entry.getKey(), entry.getValue(), false, ImmutableContext.INSTANCE, null);
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
   public CompletableFuture<Void> removeLifespanExpired(K key, V value, Long lifespan) {
      checkExpiration(getDataContainer().get(key), timeService.wallClockTime());
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletableFuture<Boolean> removeMaxIdleExpired(K key, V value) {
      if (checkExpiration(getDataContainer().get(key), timeService.wallClockTime())) {
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
   public AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> wrapper) {
      throw new UnsupportedOperationException();
   }

   @Override
   public AdvancedCache<?, ?> withMediaType(String keyMediaType, String valueMediaType) {
      throw new UnsupportedOperationException();
   }

   @Override
   public AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> keyWrapper, Class<? extends Wrapper> valueWrapper) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Encoder getKeyEncoder() {
      return keyDataConversion.getEncoder();
   }

   @Override
   public Encoder getValueEncoder() {
      return valueDataConversion.getEncoder();
   }

   @Override
   public Wrapper getKeyWrapper() {
      return keyDataConversion.getWrapper();
   }

   @Override
   public Wrapper getValueWrapper() {
      return valueDataConversion.getWrapper();
   }


   @Override
   public DataConversion getKeyDataConversion() {
      return keyDataConversion;
   }

   @Override
   public DataConversion getValueDataConversion() {
      return valueDataConversion;
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
      if (hasListeners) {
         copyEntries = new ArrayList<>(dataContainer.sizeIncludingExpired());
         dataContainer.iterator().forEachRemaining(entry -> {
            copyEntries.add(entry);
            cacheNotifier.notifyCacheEntryRemoved(entry.getKey(), entry.getValue(), entry.getMetadata(), true, ImmutableContext.INSTANCE, null);
         });
      } else {
         copyEntries = null;
      }
      dataContainer.clear();
      if (hasListeners) {
         for (InternalCacheEntry<K, V> entry : copyEntries) {
            cacheNotifier.notifyCacheEntryRemoved(entry.getKey(), entry.getValue(), entry.getMetadata(), false, ImmutableContext.INSTANCE, null);
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
      return Version.getVersion();
   }

   @ManagedAttribute(
         description = "Returns the cache configuration in form of properties",
         displayName = "Cache configuration properties",
         dataType = DataType.TRAIT,
         displayType = DisplayType.SUMMARY
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
      Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
      Objects.requireNonNull(value, NULL_VALUES_NOT_SUPPORTED);
      ValueAndMetadata<V> oldRef = new ValueAndMetadata<>();
      boolean hasListeners = this.hasListeners;
      getDataContainer().compute(key, (k, oldEntry, factory) -> {
         if (isNull(oldEntry)) {
            if (hasListeners) {
               cacheNotifier.notifyCacheEntryCreated(key, value, metadata, true, ImmutableContext.INSTANCE, null);
            }
         } else {
            oldRef.set(oldEntry.getValue(), oldEntry.getMetadata());
            if (hasListeners) {
               cacheNotifier.notifyCacheEntryModified(key, value, metadata, oldEntry.getValue(), oldEntry.getMetadata(), true, ImmutableContext.INSTANCE, null);
            }
         }
         if (oldEntry == null) {
            return factory.create(k, value, metadata);
         } else {
            return factory.update(oldEntry, value, metadata);
         }
      });
      V oldValue = oldRef.getValue();
      if (hasListeners) {
         if (oldValue == null) {
            cacheNotifier.notifyCacheEntryCreated(key, value, metadata, false, ImmutableContext.INSTANCE, null);
         } else {
            cacheNotifier.notifyCacheEntryModified(key, value, metadata, oldValue, oldRef.getMetadata(), false, ImmutableContext.INSTANCE, null);
         }
      }
      return oldValue;
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
      Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
      Objects.requireNonNull(value, NULL_VALUES_NOT_SUPPORTED);
      ByRef<V> oldValueRef = new ByRef<>(null);
      boolean hasListeners = this.hasListeners;
      getDataContainer().compute(key, (k, oldEntry, factory) -> {
         if (isNull(oldEntry)) {
            if (hasListeners) {
               cacheNotifier.notifyCacheEntryCreated(key, value, metadata, true, ImmutableContext.INSTANCE, null);
            }
            return factory.create(k, value, metadata);
         } else {
            oldValueRef.set(oldEntry.getValue());
            return oldEntry;
         }
      });
      V oldValue = oldValueRef.get();
      if (hasListeners && oldValue == null) {
         cacheNotifier.notifyCacheEntryCreated(key, value, metadata, false, ImmutableContext.INSTANCE, null);
      }
      return oldValue;
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
      Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
      Objects.requireNonNull(value, NULL_VALUES_NOT_SUPPORTED);
      ValueAndMetadata<V> oldRef = new ValueAndMetadata<>();
      boolean hasListeners = this.hasListeners;
      getDataContainer().compute(key, (k, oldEntry, factory) -> {
         if (!isNull(oldEntry)) {
            if (hasListeners) {
               cacheNotifier.notifyCacheEntryModified(key, value, metadata, oldEntry.getValue(), oldEntry.getMetadata(), true, ImmutableContext.INSTANCE, null);
            }
            oldRef.set(oldEntry.getValue(), oldEntry.getMetadata());
            return factory.update(oldEntry, value, metadata);
         } else {
            return oldEntry;
         }
      });
      V oldValue = oldRef.getValue();
      if (hasListeners && oldValue != null) {
         cacheNotifier.notifyCacheEntryModified(key, value, metadata, oldValue, oldRef.getMetadata(), false, ImmutableContext.INSTANCE, null);
      }
      return oldValue;
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
               cacheNotifier.notifyCacheEntryModified(key, value, metadata, prevValue, oldEntry.getMetadata(), true, ImmutableContext.INSTANCE, null);
            }
            return factory.update(oldEntry, value, metadata);
         } else {
            return oldEntry;
         }
      });
      if (oldRef.getValue() != null) {
         if (hasListeners) {
            cacheNotifier.notifyCacheEntryModified(key, value, metadata, oldRef.getValue(), oldRef.getMetadata(), false, ImmutableContext.INSTANCE, null);
         }
         return true;
      } else {
         return false;
      }
   }

   @Override
   public V remove(Object key) {
      Objects.requireNonNull(key, NULL_KEYS_NOT_SUPPORTED);
      ByRef<InternalCacheEntry<K, V>> oldEntryRef = new ByRef<>(null);
      boolean hasListeners = this.hasListeners;
      getDataContainer().compute((K) key, (k, oldEntry, factory) -> {
         if (!isNull(oldEntry)) {
            if (hasListeners) {
               cacheNotifier.notifyCacheEntryRemoved(oldEntry.getKey(), oldEntry.getValue(), oldEntry.getMetadata(), true, ImmutableContext.INSTANCE, null);
            }
            oldEntryRef.set(oldEntry);
         }
         return null;
      });
      InternalCacheEntry<K, V> oldEntry = oldEntryRef.get();
      if (oldEntry != null) {
         if (hasListeners) {
            cacheNotifier.notifyCacheEntryRemoved(oldEntry.getKey(), oldEntry.getValue(), oldEntry.getMetadata(), false, ImmutableContext.INSTANCE, null);
         }
         return oldEntry.getValue();
      } else {
         return null;
      }
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
   public CompletableFuture<V> removeAsync(Object key) {
      return CompletableFuture.completedFuture(remove(key));
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
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, Metadata metadata) {
      return CompletableFuture.completedFuture(replace(key, oldValue, newValue, metadata));
   }

   @Override
   public CompletableFuture<V> getAsync(K key) {
      return CompletableFuture.completedFuture(get(key));
   }

   @Override
   public boolean startBatch() {
      // invocation batching implies CacheImpl
      throw log.invocationBatchingNotEnabled();
   }

   @Override
   public void endBatch(boolean successful) {
      // invocation batching implies CacheImpl
      throw log.invocationBatchingNotEnabled();
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
               cacheNotifier.notifyCacheEntryRemoved(oldEntry.getKey(), oldValue, oldEntry.getMetadata(), true, ImmutableContext.INSTANCE, null);
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
            cacheNotifier.notifyCacheEntryRemoved(oldEntry.getKey(), oldEntry.getValue(), oldEntry.getMetadata(), false, ImmutableContext.INSTANCE, null);
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
   public void addListener(Object listener, KeyFilter<? super K> filter) {
      cacheNotifier.addListener(listener, filter);
      if (!hasListeners && canFire(listener)) {
         hasListeners = true;
      }
   }

   @Override
   public <C> void addListener(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter) {
      cacheNotifier.addListener(listener, filter, converter);
      if (!hasListeners && canFire(listener)) {
         hasListeners = true;
      }
   }

   @Override
   public void addListener(Object listener) {
      cacheNotifier.addListener(listener);
      if (!hasListeners && canFire(listener)) {
         hasListeners = true;
      }
   }

   @Override
   public void removeListener(Object listener) {
      cacheNotifier.removeListener(listener);
   }

   @Override
   public Set<Object> getListeners() {
      return cacheNotifier.getListeners();
   }

   @Override
   public <C> void addFilteredListener(Object listener,
                                       CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter,
                                       Set<Class<? extends Annotation>> filterAnnotations) {
      cacheNotifier.addFilteredListener(listener, filter, converter, filterAnnotations);
      if (!hasListeners && canFire(listener)) {
         hasListeners = true;
      }
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
   public void addInterceptor(CommandInterceptor i, int position) {
      throw log.interceptorStackNotSupported();
   }

   @Override
   public AsyncInterceptorChain getAsyncInterceptorChain() {
      return EmptyAsyncInterceptorChain.INSTANCE;
   }

   @Override
   public boolean addInterceptorAfter(CommandInterceptor i, Class<? extends CommandInterceptor> afterInterceptor) {
      throw log.interceptorStackNotSupported();
   }

   @Override
   public boolean addInterceptorBefore(CommandInterceptor i, Class<? extends CommandInterceptor> beforeInterceptor) {
      throw log.interceptorStackNotSupported();
   }

   @Override
   public void removeInterceptor(int position) {
      throw log.interceptorStackNotSupported();
   }

   @Override
   public void removeInterceptor(Class<? extends CommandInterceptor> interceptorType) {
      throw log.interceptorStackNotSupported();
   }

   @Override
   public List<CommandInterceptor> getInterceptorChain() {
      return Collections.emptyList();
   }

   @Override
   public EvictionManager getEvictionManager() {
      return getComponentRegistry().getComponent(EvictionManager.class);
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
      throw log.lockOperationsNotSupported();
   }

   @Override
   public boolean lock(Collection<? extends K> keys) {
      throw log.lockOperationsNotSupported();
   }

   @Override
   public void applyDelta(K deltaAwareValueKey, Delta delta, Object... locksToAcquire) {
      throw new UnsupportedOperationException();
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
   public InvocationContextContainer getInvocationContextContainer() {
      return null;
   }

   @Override
   public DataContainer<K, V> getDataContainer() {
      DataContainer<K, V> dataContainer = this.dataContainer;
      if (dataContainer == null) {
         ComponentStatus status = getStatus();
         switch (status) {
            case STOPPING:
               throw log.cacheIsStopping(name);
            case TERMINATED:
            case FAILED:
               throw log.cacheIsTerminated(name, status.toString());
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
   public AdvancedCache<K, V> with(ClassLoader classLoader) {
      return this;
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
                  cacheNotifier.notifyCacheEntryCreated(k, newValue, metadata, true, ImmutableContext.INSTANCE, null);
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
         cacheNotifier.notifyCacheEntryCreated(key, newValueRef.get(), metadata, false, ImmutableContext.INSTANCE, null);
      }
      return returnEntry == null ? null : returnEntry.getValue();
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      CacheEntryChange<K, V> ref = new CacheEntryChange<>();
      return computeIfPresentInternal(key, remappingFunction, ref, defaultMetadata);
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
                  cacheNotifier.notifyCacheEntryRemoved(k, oldValue, oldEntry.getMetadata(), true, ImmutableContext.INSTANCE, null);
               }
               ref.set(k, null, oldValue, oldEntry.getMetadata());
               return null;
            } else {
               if (hasListeners) {
                  cacheNotifier.notifyCacheEntryModified(k, newValue, metadata, oldValue, oldEntry.getMetadata(), true, ImmutableContext.INSTANCE, null);
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
            cacheNotifier.notifyCacheEntryModified(ref.getKey(), newValue, metadata, ref.getOldValue(), ref.getOldMetadata(), false, ImmutableContext.INSTANCE, null);
         } else {
            cacheNotifier.notifyCacheEntryRemoved(ref.getKey(), ref.getOldValue(), ref.getOldMetadata(), false, ImmutableContext.INSTANCE, null);
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
         return getUpdatedEntry(k, oldEntry, factory, oldValue, newValue, ref, hasListeners);
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
         return getUpdatedEntry(k, oldEntry, factory, oldValue, newValue, ref, hasListeners);
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
               cacheNotifier.notifyCacheEntryRemoved(key, oldValue, ref.getOldMetadata(), false, ImmutableContext.INSTANCE, null);
            } else if (oldValue == null) {
               cacheNotifier.notifyCacheEntryCreated(key, newValue, metadata, false, ImmutableContext.INSTANCE, null);
            } else {
               cacheNotifier.notifyCacheEntryModified(key, newValue, metadata, oldValue, ref.getOldMetadata(), false, ImmutableContext.INSTANCE, null);
            }
         }
      }
      return newValue;
   }

   private InternalCacheEntry<K, V> getUpdatedEntry(K k, InternalCacheEntry<K, V> oldEntry, InternalEntryFactory factory, V oldValue, V newValue, CacheEntryChange<K, V> ref, boolean hasListeners) {
      if (newValue == null) {
         if (oldValue != null) {
            if (hasListeners) {
               cacheNotifier.notifyCacheEntryRemoved(k, oldValue, oldEntry.getMetadata(), true, ImmutableContext.INSTANCE, null);
            }
            ref.set(k, null, oldValue, oldEntry.getMetadata());
         }
         return null;
      } else if (oldValue == null) {
         if (hasListeners) {
            cacheNotifier.notifyCacheEntryCreated(k, newValue, defaultMetadata, true, ImmutableContext.INSTANCE, null);
         }
         ref.set(k, newValue, null, null);
         return factory.create(k, newValue, defaultMetadata);
      } else if (Objects.equals(oldValue, newValue)) {
         return oldEntry;
      } else {
         if (hasListeners) {
            cacheNotifier.notifyCacheEntryModified(k, newValue, defaultMetadata, oldValue, oldEntry.getMetadata(), true, ImmutableContext.INSTANCE, null);
         }
         ref.set(k, newValue, oldValue, oldEntry.getMetadata());
         return factory.update(oldEntry, newValue, defaultMetadata);
      }
   }

   // This method can be called only from dataContainer.compute()'s action;
   // as we'll replace the old value when it's expired
   private boolean isNull(InternalCacheEntry<K, V> entry) {
      if (entry == null) {
         return true;
      } else if (entry.canExpire()) {
         if (entry.isExpired(timeService.wallClockTime())) {
            cacheNotifier.notifyCacheEntryExpired(entry.getKey(), entry.getValue(), entry.getMetadata(), ImmutableContext.INSTANCE);
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
      boolean hasListeners = this.hasListeners;
      CacheEntryChange<K, V> ref = new CacheEntryChange<>();
      for (Iterator<InternalCacheEntry<K, V>> it = dataContainer.iterator(); it.hasNext(); ) {
         InternalCacheEntry<K, V> ice = it.next();
         getDataContainer().compute(ice.getKey(), (k, oldEntry, factory) -> {
            V oldValue = getValue(oldEntry);
            if (oldValue != null) {
               V newValue = function.apply(k, oldValue);
               Objects.requireNonNull(newValue, NULL_VALUES_NOT_SUPPORTED);
               if (hasListeners) {
                  cacheNotifier.notifyCacheEntryModified(k, newValue, defaultMetadata, oldValue, oldEntry.getMetadata(),
                        true, ImmutableContext.INSTANCE, null);
               }
               ref.set(k, newValue, oldValue, oldEntry.getMetadata());
               return factory.update(oldEntry, newValue, defaultMetadata);
            } else {
               return null;
            }
         });
         if (hasListeners) {
            cacheNotifier.notifyCacheEntryModified(ref.getKey(), ref.getNewValue(), defaultMetadata, ref.getOldValue(),
                  ref.getOldMetadata(), false, ImmutableContext.INSTANCE, null);
         }
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

   protected abstract class EntrySetBase<T extends Entry<K, V>> implements CacheSet<T> {
      private final Set<? extends Entry<K, V>> delegate = getDataContainer().entrySet();

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
         return delegate.contains(o);
      }

      @Override
      public Object[] toArray() {
         return delegate.toArray();
      }

      @Override
      public <U> U[] toArray(U[] a) {
         return delegate.toArray(a);
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
      public boolean containsAll(Collection<?> c) {
         return delegate.containsAll(c);
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

   protected class Values extends CloseableIteratorCollectionAdapter<V> implements CacheCollection<V> {
      public Values() {
         super(getDataContainer().values());
      }

      @Override
      public boolean retainAll(Collection<?> c) {
         Set<Object> retained = CollectionFactory.makeSet(c.size());
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
         Set<Object> removed = CollectionFactory.makeSet(removeSize);
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

   protected class KeySet extends CloseableIteratorSetAdapter<K> implements CacheSet<K> {
      public KeySet() {
         super(getDataContainer().keySet());
      }

      @Override
      public boolean retainAll(Collection<?> c) {
         Set<Object> retained = CollectionFactory.makeSet(c.size());
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
   public String toString() {
      return "SimpleCache '" + getName() + "'@" + Util.hexIdHashCode(getCacheManager());
   }
}
