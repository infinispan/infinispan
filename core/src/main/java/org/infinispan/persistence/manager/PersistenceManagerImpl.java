package org.infinispan.persistence.manager;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.infinispan.context.Flag.IGNORE_RETURN_VALUES;
import static org.infinispan.context.Flag.SKIP_CACHE_STORE;
import static org.infinispan.context.Flag.SKIP_INDEXING;
import static org.infinispan.context.Flag.SKIP_LOCKING;
import static org.infinispan.context.Flag.SKIP_OWNERSHIP_CHECK;
import static org.infinispan.context.Flag.SKIP_XSITE_BACKUP;
import static org.infinispan.factories.KnownComponentNames.PERSISTENCE_EXECUTOR;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Features;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.AbstractSegmentedStoreConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.context.Flag;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.eviction.EvictionType;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.factories.DataContainerFactory;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.CacheLoaderInterceptor;
import org.infinispan.interceptors.impl.CacheWriterInterceptor;
import org.infinispan.interceptors.impl.TransactionalStoreInterceptor;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.persistence.InitializationContextImpl;
import org.infinispan.persistence.async.AdvancedAsyncCacheLoader;
import org.infinispan.persistence.async.AdvancedAsyncCacheWriter;
import org.infinispan.persistence.async.AsyncCacheLoader;
import org.infinispan.persistence.async.AsyncCacheWriter;
import org.infinispan.persistence.async.State;
import org.infinispan.persistence.factory.CacheStoreFactoryRegistry;
import org.infinispan.persistence.internal.PersistenceUtil;
import org.infinispan.persistence.spi.AdvancedCacheExpirationWriter;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.AdvancedCacheWriter;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.persistence.spi.FlagAffectedStore;
import org.infinispan.persistence.spi.LocalOnlyCacheLoader;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.spi.SegmentedAdvancedLoadWriteStore;
import org.infinispan.persistence.spi.StoreUnavailableException;
import org.infinispan.persistence.spi.TransactionalCacheWriter;
import org.infinispan.persistence.support.AdvancedSingletonCacheWriter;
import org.infinispan.persistence.support.BatchModification;
import org.infinispan.persistence.support.ComposedSegmentedLoadWriteStore;
import org.infinispan.persistence.support.DelegatingCacheLoader;
import org.infinispan.persistence.support.DelegatingCacheWriter;
import org.infinispan.persistence.support.SingletonCacheWriter;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.reactivex.internal.functions.Functions;
import net.jcip.annotations.GuardedBy;

public class PersistenceManagerImpl implements PersistenceManager {

   private static final Log log = LogFactory.getLog(PersistenceManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject private Configuration configuration;
   @Inject private GlobalConfiguration globalConfiguration;
   @Inject private ComponentRef<AdvancedCache<Object, Object>> cache;
   @Inject private StreamingMarshaller m;
   @Inject private TransactionManager transactionManager;
   @Inject private TimeService timeService;
   @Inject @ComponentName(PERSISTENCE_EXECUTOR)
   private ScheduledExecutorService persistenceExecutor;
   @Inject private ByteBufferFactory byteBufferFactory;
   @Inject private MarshalledEntryFactory marshalledEntryFactory;
   @Inject private CacheStoreFactoryRegistry cacheStoreFactoryRegistry;
   @Inject private ComponentRef<InternalExpirationManager<Object, Object>> expirationManager;
   @Inject private CacheNotifier cacheNotifier;
   @Inject private KeyPartitioner keyPartitioner;
   @Inject private Transport transport;

   @GuardedBy("storesMutex")
   private final List<CacheLoader> loaders = new ArrayList<>();
   @GuardedBy("storesMutex")
   private final List<CacheWriter> nonTxWriters = new ArrayList<>();
   @GuardedBy("storesMutex")
   private final List<TransactionalCacheWriter> txWriters = new ArrayList<>();
   private final Semaphore publisherSemaphore = new Semaphore(Integer.MAX_VALUE);
   private final ReadWriteLock storesMutex = new ReentrantReadWriteLock();
   private final Lock availabilityMutex = new ReentrantLock();
   @GuardedBy("storesMutex")
   private final Map<Object, StoreStatus> storeStatuses = new HashMap<>();
   private AdvancedPurgeListener<Object, Object> advancedListener;
   private final Callable<Semaphore> publisherSemaphoreCallable = Functions.justCallable(publisherSemaphore);

   /**
    * making it volatile as it might change after @Start, so it needs the visibility.
    */
   private volatile boolean enabled;
   private volatile boolean clearOnStop;
   private boolean preloaded;
   private Future availabilityFuture;
   private volatile StoreUnavailableException unavailableException;

   @Override
   @Start()
   public void start() {
      advancedListener = new AdvancedPurgeListener<>(expirationManager.wired());
      preloaded = false;
      enabled = configuration.persistence().usingStores();
      if (!enabled)
         return;
      try {
         createLoadersAndWriters();
         Transaction xaTx = null;
         if (transactionManager != null) {
            xaTx = transactionManager.suspend();
         }
         storesMutex.writeLock().lock();
         try {
            Set<Lifecycle> undelegated = new HashSet<>();
            nonTxWriters.forEach(w -> startWriter(w, undelegated));
            txWriters.forEach(w -> startWriter(w, undelegated));
            loaders.forEach(l -> startLoader(l, undelegated));

            // Ensure that after writers and loaders have started, they are classified as available by their isAvailable impl
            pollStoreAvailability();

            // Now schedule the availability check
            long interval = configuration.persistence().availabilityInterval();
            if (interval > 0)
               availabilityFuture = persistenceExecutor.scheduleAtFixedRate(this::pollStoreAvailability, interval, interval, TimeUnit.MILLISECONDS);
         } finally {
            if (xaTx != null) {
               transactionManager.resume(xaTx);
            }
            storesMutex.writeLock().unlock();
         }
      } catch (Exception e) {
         throw new CacheException("Unable to start cache loaders", e);
      }
   }

   protected void pollStoreAvailability() {
      storesMutex.readLock().lock();
      try {
         // If the lock is not available, then it means that an availability check is in progress due to scheduled calls overlapping
         if (availabilityMutex.tryLock()) {
            try {
               boolean availabilityChanged = false;
               boolean failureDetected = false;
               for (StoreStatus status : storeStatuses.values()) {
                  if (status.availabilityChanged())
                     availabilityChanged = true;
                  if (availabilityChanged && !status.availability && !failureDetected) {
                     failureDetected = true;
                     unavailableException = new StoreUnavailableException(String.format("Store %s is unavailable", status.store));
                     cacheNotifier.notifyPersistenceAvailabilityChanged(false);
                  }
               }
               if (!failureDetected && availabilityChanged) {
                  unavailableException = null;
                  cacheNotifier.notifyPersistenceAvailabilityChanged(true);
               }
            } finally {
               availabilityMutex.unlock();
            }
         }
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   @Stop
   public void stop() {
      storesMutex.writeLock().lock();
      publisherSemaphore.acquireUninterruptibly(Integer.MAX_VALUE);
      try {
         // If needed, clear the persistent store before stopping
         if (clearOnStop) {
            clearAllStores(AccessMode.BOTH);
         }

         Set<Lifecycle> undelegated = new HashSet<>();
         Consumer<CacheWriter> stopWriters = writer -> {
            writer.stop();
            if (writer instanceof DelegatingCacheWriter) {
               CacheWriter actual = undelegate(writer);
               actual.stop();
               undelegated.add(actual);
            } else {
               undelegated.add(writer);
            }
         };
         if (availabilityFuture != null)
            availabilityFuture.cancel(true);
         nonTxWriters.forEach(stopWriters);
         nonTxWriters.clear();
         txWriters.forEach(stopWriters);
         txWriters.clear();

         for (CacheLoader l : loaders) {
            if (!undelegated.contains(l)) {
               l.stop();
            }
            if (l instanceof DelegatingCacheLoader) {
               CacheLoader actual = undelegate(l);
               if (!undelegated.contains(actual)) {
                  actual.stop();
               }
            }
         }
         loaders.clear();
         preloaded = false;
      } finally {
         publisherSemaphore.release(Integer.MAX_VALUE);
         storesMutex.writeLock().unlock();
      }
   }

   @Override
   public boolean isEnabled() {
      return enabled;
   }

   private void checkStoreAvailability() {
      if (!enabled) return;

      if (unavailableException != null) {
         throw unavailableException;
      }
   }

   @Override
   public boolean isAvailable() {
      if (!enabled)
         return false;
      return unavailableException == null;
   }

   @Override
   public boolean isPreloaded() {
      return preloaded;
   }

   @Override
   public void preload() {
      if (!enabled)
         return;
      AdvancedCacheLoader<Object, Object> preloadCl = null;

      storesMutex.readLock().lock();
      try {
         for (CacheLoader l : loaders) {
            if (getStoreConfig(l).preload()) {
               if (!(l instanceof AdvancedCacheLoader)) {
                  throw new PersistenceException("Cannot preload from cache loader '" + l.getClass().getName()
                        + "' as it doesn't implement '" + AdvancedCacheLoader.class.getName() + "'");
               }
               preloadCl = (AdvancedCacheLoader) l;
               if (preloadCl instanceof AdvancedAsyncCacheLoader)
                  preloadCl = (AdvancedCacheLoader) ((AdvancedAsyncCacheLoader) preloadCl).undelegate();
               break;
            }
         }
      } finally {
         storesMutex.readLock().unlock();
      }
      if (preloadCl == null)
         return;

      long start = timeService.time();


      final long maxEntries = getMaxEntries();
      final AtomicInteger loadedEntries = new AtomicInteger(0);
      final AdvancedCache<Object, Object> flaggedCache = getCacheForStateInsertion();
      Long insertAmount = Flowable.fromPublisher(preloadCl.publishEntries(null, true, true))
            .take(maxEntries)
            .doOnNext(me -> {
               //the downcast will go away with ISPN-3460
               Metadata metadata = me.getMetadata() != null ? ((InternalMetadataImpl) me.getMetadata()).actual() : null;
               preloadKey(flaggedCache, me.getKey(), me.getValue(), metadata);
            }).count().blockingGet();
      this.preloaded = insertAmount < maxEntries;

      log.debugf("Preloaded %d keys in %s", loadedEntries.get(), Util.prettyPrintTime(timeService.timeDuration(start, MILLISECONDS)));
   }

   @Override
   public void disableStore(String storeType) {
      if (enabled) {
         boolean noMoreStores;
         storesMutex.writeLock().lock();
         publisherSemaphore.acquireUninterruptibly(Integer.MAX_VALUE);
         try {
            removeCacheLoader(storeType, loaders);
            removeCacheWriter(storeType, nonTxWriters);
            removeCacheWriter(storeType, txWriters);
            noMoreStores = loaders.isEmpty() && nonTxWriters.isEmpty() && txWriters.isEmpty();

            if (!noMoreStores) {
               // Immediately poll store availability as the disabled store may have been the cause of the unavailability
               pollStoreAvailability();
            }
         } finally {
            publisherSemaphore.release(Integer.MAX_VALUE);
            storesMutex.writeLock().unlock();
         }

         if (noMoreStores) {
            if (availabilityFuture != null)
               availabilityFuture.cancel(true);

            AsyncInterceptorChain chain = cache.wired().getAsyncInterceptorChain();
            AsyncInterceptor loaderInterceptor = chain.findInterceptorExtending(CacheLoaderInterceptor.class);
            if (loaderInterceptor == null) {
               log.persistenceWithoutCacheLoaderInterceptor();
            } else {
               chain.removeInterceptor(loaderInterceptor.getClass());
            }
            AsyncInterceptor writerInterceptor = chain.findInterceptorExtending(CacheWriterInterceptor.class);
            if (writerInterceptor == null) {
               writerInterceptor = chain.findInterceptorWithClass(TransactionalStoreInterceptor.class);
               if (writerInterceptor == null) {
                  log.persistenceWithoutCacheWriteInterceptor();
               } else {
                  chain.removeInterceptor(writerInterceptor.getClass());
               }
            } else {
               chain.removeInterceptor(writerInterceptor.getClass());
            }
            enabled = false;
         }
      }
   }

   @Override
   public <T> Set<T> getStores(Class<T> storeClass) {
      storesMutex.readLock().lock();
      try {
         Set<T> result = new HashSet<>();
         for (CacheLoader l : loaders) {
            CacheLoader real = undelegate(l);
            if (storeClass.isInstance(real)) {
               result.add(storeClass.cast(real));
            }
         }

         Consumer<CacheWriter> getWriters = writer -> {
            CacheWriter real = undelegate(writer);
            if (storeClass.isInstance(real)) {
               result.add(storeClass.cast(real));
            }
         };
         nonTxWriters.forEach(getWriters);
         txWriters.forEach(getWriters);

         return result;
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public Collection<String> getStoresAsString() {
      storesMutex.readLock().lock();
      try {
         Set<String> loaderTypes = new HashSet<>(loaders.size());
         for (CacheLoader loader : loaders)
            loaderTypes.add(undelegate(loader).getClass().getName());
         for (CacheWriter writer : nonTxWriters)
            loaderTypes.add(undelegate(writer).getClass().getName());
         for (CacheWriter writer : txWriters)
            loaderTypes.add(undelegate(writer).getClass().getName());
         return loaderTypes;
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   private static class AdvancedPurgeListener<K, V> implements AdvancedCacheExpirationWriter.ExpirationPurgeListener<K, V> {
      private final InternalExpirationManager<K, V> expirationManager;

      private AdvancedPurgeListener(InternalExpirationManager<K, V> expirationManager) {
         this.expirationManager = expirationManager;
      }

      @Override
      public void marshalledEntryPurged(MarshalledEntry<K, V> entry) {
         expirationManager.handleInStoreExpiration(entry);
      }

      @Override
      public void entryPurged(K key) {
         expirationManager.handleInStoreExpiration(key);
      }
   }

   @Override
   public void purgeExpired() {
      if (!enabled)
         return;
      long start = -1;
      try {
         if (trace) {
            log.trace("Purging cache store of expired entries");
            start = timeService.time();
         }

         storesMutex.readLock().lock();
         try {
            checkStoreAvailability();
            Consumer<CacheWriter> purgeWriter = writer -> {
               // ISPN-6711 Shared stores should only be purged by the coordinator
               if (getStoreConfig(writer).shared() && !transport.isCoordinator())
                  return;

               if (writer instanceof AdvancedCacheExpirationWriter) {
                  //noinspection unchecked
                  ((AdvancedCacheExpirationWriter)writer).purge(persistenceExecutor, advancedListener);
               } else if (writer instanceof AdvancedCacheWriter) {
                  //noinspection unchecked
                  ((AdvancedCacheWriter)writer).purge(persistenceExecutor, advancedListener);
               }
            };
            nonTxWriters.forEach(purgeWriter);
            txWriters.forEach(purgeWriter);
         } finally {
            storesMutex.readLock().unlock();
         }

         if (trace) {
            log.tracef("Purging cache store completed in %s",
                  Util.prettyPrintTime(timeService.timeDuration(start, TimeUnit.MILLISECONDS)));
         }
      } catch (Exception e) {
         log.exceptionPurgingDataContainer(e);
      }
   }

   @Override
   public void clearAllStores(Predicate<? super StoreConfiguration> predicate) {
      storesMutex.readLock().lock();
      try {
         checkStoreAvailability();
         // Apply to txWriters as well as clear does not happen in a Tx context
         Consumer<CacheWriter> clearWriter = writer -> {
            if (writer instanceof AdvancedCacheWriter) {
               if (predicate.test(getStoreConfig(writer))) {
                  ((AdvancedCacheWriter) writer).clear();
               }
            }
         };
         nonTxWriters.forEach(clearWriter);
         txWriters.forEach(clearWriter);
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public boolean deleteFromAllStores(Object key, int segment, Predicate<? super StoreConfiguration> predicate) {
      storesMutex.readLock().lock();
      try {
         checkStoreAvailability();
         boolean removed = false;
         for (CacheWriter w : nonTxWriters) {
            if (predicate.test(getStoreConfig(w))) {
               if (w instanceof SegmentedAdvancedLoadWriteStore) {
                  removed |= ((SegmentedAdvancedLoadWriteStore) w).delete(segment, key);
               } else {
                  removed |= w.delete(key);
               }
            }
         }
         return removed;
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   <K, V> AdvancedCacheLoader<K, V> getFirstAdvancedCacheLoader(Predicate<? super StoreConfiguration> predicate) {
      storesMutex.readLock().lock();
      try {
         for (CacheLoader loader : loaders) {
            if (predicate.test(getStoreConfig(loader)) && loader instanceof AdvancedCacheLoader) {
               return ((AdvancedCacheLoader<K, V>) loader);
            }
         }
      } finally {
         storesMutex.readLock().unlock();
      }
      return null;
   }

   <K, V> SegmentedAdvancedLoadWriteStore<K, V> getFirstSegmentedStore(Predicate<? super StoreConfiguration> predicate) {
      storesMutex.readLock().lock();
      try {
         for (CacheLoader l : loaders) {
            StoreConfiguration storeConfiguration;
            if (l instanceof SegmentedAdvancedLoadWriteStore &&
                  (storeConfiguration = getStoreConfig(l)) != null && storeConfiguration.segmented() &&
                  predicate.test(storeConfiguration)) {
               return ((SegmentedAdvancedLoadWriteStore<K, V>) l);
            }
         }
      } finally {
         storesMutex.readLock().unlock();
      }
      return null;
   }

   @Override
   public <K, V> Publisher<MarshalledEntry<K, V>> publishEntries(Predicate<? super K> filter, boolean fetchValue,
         boolean fetchMetadata, Predicate<? super StoreConfiguration> predicate) {
      AdvancedCacheLoader<K, V> advancedCacheLoader = getFirstAdvancedCacheLoader(predicate);

      if (advancedCacheLoader != null) {
         // We have to acquire the read lock on the stores mutex to be sure that no concurrent stop or store removal
         // is done while processing data
         return Flowable.using(publisherSemaphoreCallable, semaphore -> {
            semaphore.acquire();
            return advancedCacheLoader.publishEntries(filter, fetchValue, fetchMetadata);
         }, Semaphore::release);
      }
      return Flowable.empty();
   }

   @Override
   public <K, V> Publisher<MarshalledEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter,
         boolean fetchValue, boolean fetchMetadata, Predicate<? super StoreConfiguration> predicate) {
      SegmentedAdvancedLoadWriteStore<K, V> segmentedStore = getFirstSegmentedStore(predicate);
      if (segmentedStore != null) {
         return Flowable.using(publisherSemaphoreCallable, semaphore -> {
            semaphore.acquire();
            return segmentedStore.publishEntries(segments, filter, fetchValue, fetchMetadata);
         }, Semaphore::release);
      }
      return publishEntries(PersistenceUtil.combinePredicate(segments, keyPartitioner, filter), fetchValue, fetchMetadata, predicate);
   }

   @Override
   public <K> Publisher<K> publishKeys(Predicate<? super K> filter, Predicate<? super StoreConfiguration> predicate) {
      AdvancedCacheLoader<K, ?> advancedCacheLoader = getFirstAdvancedCacheLoader(predicate);

      if (advancedCacheLoader != null) {
         // We have to acquire the read lock on the stores mutex to be sure that no concurrent stop or store removal
         // is done while processing data
         return Flowable.using(publisherSemaphoreCallable, semaphore -> {
            semaphore.acquire();
            return advancedCacheLoader.publishKeys(filter);
         }, Semaphore::release);
      }
      return Flowable.empty();
   }

   @Override
   public <K> Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter,
         Predicate<? super StoreConfiguration> predicate) {
      SegmentedAdvancedLoadWriteStore<K, ?> segmentedStore = getFirstSegmentedStore(predicate);

      if (segmentedStore != null) {
         // We have to acquire the read lock on the stores mutex to be sure that no concurrent stop or store removal
         // is done while processing data
         return Flowable.using(publisherSemaphoreCallable, semaphore -> {
            semaphore.acquire();
            return segmentedStore.publishKeys(segments, filter);
         }, Semaphore::release);
      }

      return publishKeys(PersistenceUtil.combinePredicate(segments, keyPartitioner, filter), predicate);
   }

   @Override
   public MarshalledEntry loadFromAllStores(Object key, boolean localInvocation, boolean includeStores) {
      storesMutex.readLock().lock();
      try {
         checkStoreAvailability();
         for (CacheLoader l : loaders) {
            if (allowLoad(l, localInvocation, includeStores)) {
               MarshalledEntry load = l.load(key);
               if (load != null)
                  return load;
            }
         }
         return null;
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public MarshalledEntry loadFromAllStores(Object key, int segment, boolean localInvocation, boolean includeStores) {
      storesMutex.readLock().lock();
      try {
         checkStoreAvailability();
         for (CacheLoader l : loaders) {
            if (allowLoad(l, localInvocation, includeStores) && l instanceof SegmentedAdvancedLoadWriteStore) {
               MarshalledEntry load = ((SegmentedAdvancedLoadWriteStore) l).load(segment, key);
               if (load != null)
                  return load;
            }
         }
         for (CacheLoader l : loaders) {
            if (allowLoad(l, localInvocation, includeStores)) {
               MarshalledEntry load = l.load(key);
               if (load != null)
                  return load;
            }
         }
         return null;
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   private boolean allowLoad(CacheLoader loader, boolean localInvocation, boolean includeStores) {
      return (localInvocation || !isLocalOnlyLoader(loader)) && (includeStores || !(loader instanceof CacheWriter));
   }

   private boolean isLocalOnlyLoader(CacheLoader loader) {
      if (loader instanceof LocalOnlyCacheLoader) return true;
      if (loader instanceof DelegatingCacheLoader) {
         CacheLoader unwrappedLoader = ((DelegatingCacheLoader) loader).undelegate();
         return unwrappedLoader instanceof LocalOnlyCacheLoader;
      }
      return false;
   }

   @Override
   public void writeToAllNonTxStores(MarshalledEntry marshalledEntry, int segment,
         Predicate<? super StoreConfiguration> predicate) {
      writeToAllNonTxStores(marshalledEntry, segment, predicate, 0L);
   }

   @Override
   public void writeToAllNonTxStores(MarshalledEntry marshalledEntry, int segment,
         Predicate<? super StoreConfiguration> predicate, long flags) {
      storesMutex.readLock().lock();
      try {
         checkStoreAvailability();
         //noinspection unchecked
         nonTxWriters.stream()
               .filter(writer -> !(writer instanceof FlagAffectedStore) || FlagAffectedStore.class.cast(writer).shouldWrite(flags))
               .filter(writer -> predicate.test(getStoreConfig(writer)))
               .forEach(writer -> {
                  if (writer instanceof SegmentedAdvancedLoadWriteStore) {
                     ((SegmentedAdvancedLoadWriteStore) writer).write(segment, marshalledEntry);
                  } else {
                     writer.write(marshalledEntry);
                  }
               });
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public void writeBatchToAllNonTxStores(Iterable<MarshalledEntry> entries,
         Predicate<? super StoreConfiguration> predicate, long flags) {
      if (!entries.iterator().hasNext())
         return;

      storesMutex.readLock().lock();
      try {
         checkStoreAvailability();
         //noinspection unchecked
         nonTxWriters.stream()
               .filter(writer -> !(writer instanceof FlagAffectedStore) || FlagAffectedStore.class.cast(writer).shouldWrite(flags))
               .filter(writer -> predicate.test(getStoreConfig(writer)))
               .forEach(writer -> writer.writeBatch(entries));
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public void deleteBatchFromAllNonTxStores(Iterable<Object> keys, Predicate<? super StoreConfiguration> predicate,
         long flags) {
      if (!keys.iterator().hasNext())
         return;

      storesMutex.readLock().lock();
      try {
         checkStoreAvailability();
         nonTxWriters.stream()
               .filter(writer -> predicate.test(getStoreConfig(writer)))
               .forEach(writer -> writer.deleteBatch(keys));
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public void prepareAllTxStores(Transaction transaction, BatchModification batchModification,
         Predicate<? super StoreConfiguration> predicate) throws PersistenceException {
      storesMutex.readLock().lock();
      try {
         checkStoreAvailability();
         for (CacheWriter writer : txWriters) {
            if (predicate.test(getStoreConfig(writer)) || configuration.clustering().cacheMode().equals(CacheMode.LOCAL)) {
               TransactionalCacheWriter txWriter = (TransactionalCacheWriter) undelegate(writer);
               txWriter.prepareWithModifications(transaction, batchModification);
            }
         }
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public void commitAllTxStores(Transaction transaction, Predicate<? super StoreConfiguration> predicate) {
      performOnAllTxStores(predicate, writer -> writer.commit(transaction));
   }

   @Override
   public void rollbackAllTxStores(Transaction transaction, Predicate<? super StoreConfiguration> predicate) {
      performOnAllTxStores(predicate, writer -> writer.rollback(transaction));
   }

   @Override
   public AdvancedCacheLoader getStateTransferProvider() {
      storesMutex.readLock().lock();
      try {
         checkStoreAvailability();
         for (CacheLoader l : loaders) {
            StoreConfiguration storeConfiguration = getStoreConfig(l);
            if (storeConfiguration.fetchPersistentState() && !storeConfiguration.shared())
               return (AdvancedCacheLoader) l;
         }
         return null;
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public int size(Predicate<? super StoreConfiguration> predicate) {
      storesMutex.readLock().lock();
      try {
         checkStoreAvailability();
         for (CacheLoader l : loaders) {
            StoreConfiguration storeConfiguration = getStoreConfig(l);
            if (predicate.test(storeConfiguration) && l instanceof AdvancedCacheLoader)
               return ((AdvancedCacheLoader) l).size();
         }
      } finally {
         storesMutex.readLock().unlock();
      }
      return -1;
   }

   @Override
   public int size(IntSet segments) {
      storesMutex.readLock().lock();
      try {
         checkStoreAvailability();
         for (CacheLoader l : loaders) {
            StoreConfiguration storeConfiguration;
            if (l instanceof SegmentedAdvancedLoadWriteStore &&
                  ((storeConfiguration = getStoreConfig(l)) != null && storeConfiguration.segmented())) {
               return ((SegmentedAdvancedLoadWriteStore) l).size(segments);
            }
         }
         long count = Flowable.fromPublisher(publishKeys(segments, null, AccessMode.BOTH))
               .count().blockingGet();
         if (count > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
         }
         return (int) count;
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public void setClearOnStop(boolean clearOnStop) {
      this.clearOnStop = clearOnStop;
   }

   @Override
   public boolean addSegments(IntSet segments) {
      boolean allSegmented = true;
      storesMutex.readLock().lock();
      try {
         for (CacheLoader loader : loaders) {
            if (AccessMode.PRIVATE.test(getStoreConfig(loader))) {
               if (loader instanceof SegmentedAdvancedLoadWriteStore) {
                  ((SegmentedAdvancedLoadWriteStore) loader).addSegments(segments);
               } else if (loader instanceof CacheWriter) {
                  allSegmented = false;
               }
            }
         }
      } finally {
         storesMutex.readLock().unlock();
      }
      return allSegmented;
   }

   @Override
   public boolean removeSegments(IntSet segments) {
      boolean allSegmented = true;
      storesMutex.readLock().lock();
      try {
         for (CacheLoader loader : loaders) {
            if (AccessMode.PRIVATE.test(getStoreConfig(loader))) {
               if (loader instanceof SegmentedAdvancedLoadWriteStore) {
                  ((SegmentedAdvancedLoadWriteStore) loader).removeSegments(segments);
               } else if (loader instanceof CacheWriter) {
                  allSegmented = false;
               }
            }
         }
      } finally {
         storesMutex.readLock().unlock();
      }
      return allSegmented;
   }

   public List<CacheLoader> getAllLoaders() {
      storesMutex.readLock().lock();
      try {
         return new ArrayList<>(loaders);
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   public List<CacheWriter> getAllWriters() {
      storesMutex.readLock().lock();
      try {
         return new ArrayList<>(nonTxWriters);
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   public List<CacheWriter> getAllTxWriters() {
      storesMutex.readLock().lock();
      try {
         return new ArrayList<>(txWriters);
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   private void createLoadersAndWriters() {
      Features features = globalConfiguration.features();
      for (StoreConfiguration cfg : configuration.persistence().stores()) {

         final Object bareInstance;
         if (cfg.segmented()) {
            if (!features.isAvailable(DataContainerFactory.SEGMENTATION_FEATURE)) {
               throw org.infinispan.commons.logging.LogFactory.getLog(MethodHandles.lookup().lookupClass())
                     .featureDisabled(DataContainerFactory.SEGMENTATION_FEATURE);
            }
            if (cfg instanceof AbstractSegmentedStoreConfiguration) {
               bareInstance = new ComposedSegmentedLoadWriteStore<>((AbstractSegmentedStoreConfiguration) cfg);
            } else {
               bareInstance = cacheStoreFactoryRegistry.createInstance(cfg);
            }
         } else {
            bareInstance = cacheStoreFactoryRegistry.createInstance(cfg);
         }

         StoreConfiguration processedConfiguration = cacheStoreFactoryRegistry.processStoreConfiguration(cfg);

         CacheWriter writer = createCacheWriter(bareInstance);
         CacheLoader loader = createCacheLoader(bareInstance);

         writer = postProcessWriter(processedConfiguration, writer);
         loader = postProcessReader(processedConfiguration, writer, loader);

         InitializationContextImpl ctx =
            new InitializationContextImpl(processedConfiguration, cache.wired(), keyPartitioner, m, timeService,
                                          byteBufferFactory, marshalledEntryFactory, persistenceExecutor);
         initializeLoader(processedConfiguration, loader, ctx);
         initializeWriter(processedConfiguration, writer, ctx);
         initializeBareInstance(bareInstance, ctx);
      }
   }

   private CacheLoader postProcessReader(StoreConfiguration cfg, CacheWriter writer, CacheLoader loader) {
      if(cfg.async().enabled() && loader != null && writer != null) {
         loader = createAsyncLoader(loader, (AsyncCacheWriter) writer);
      }
      return loader;
   }

   private CacheWriter postProcessWriter(StoreConfiguration cfg, CacheWriter writer) {
      if (writer != null) {
         if(cfg.ignoreModifications()) {
            writer = null;
         } else if (cfg.singletonStore().enabled()) {
            writer = createSingletonWriter(cfg, writer);
         } else if (cfg.async().enabled()) {
            writer = createAsyncWriter(writer);
         }
      }
      return writer;
   }

   private CacheLoader createAsyncLoader(CacheLoader loader, AsyncCacheWriter asyncWriter) {
      AtomicReference<State> state = asyncWriter.getState();
      loader = (loader instanceof AdvancedCacheLoader) ?
            new AdvancedAsyncCacheLoader(loader, state) : new AsyncCacheLoader(loader, state);
      return loader;
   }

   private SingletonCacheWriter createSingletonWriter(StoreConfiguration cfg, CacheWriter writer) {
      return (writer instanceof AdvancedCacheWriter) ?
            new AdvancedSingletonCacheWriter(writer, cfg.singletonStore()) :
            new SingletonCacheWriter(writer, cfg.singletonStore());
   }

   private void initializeWriter(StoreConfiguration cfg, CacheWriter writer, InitializationContextImpl ctx) {
      if (writer != null) {
         if (writer instanceof DelegatingCacheWriter)
            writer.init(ctx);

         storesMutex.writeLock().lock();
         try {
            if (undelegate(writer) instanceof TransactionalCacheWriter && cfg.transactional()) {
               if (configuration.transaction().transactionMode().isTransactional()) {
                  txWriters.add((TransactionalCacheWriter) writer);
               } else {
                  // If cache is non-transactional then it is not possible for the store to be, so treat as normal store
                  // Shouldn't happen as a CacheConfigurationException should be thrown on validation
                  nonTxWriters.add(writer);
               }
            } else {
               nonTxWriters.add(writer);
            }
            storeStatuses.put(writer, new StoreStatus(writer, cfg));
         } finally {
            storesMutex.writeLock().unlock();
         }
      }
   }

   private void initializeLoader(StoreConfiguration cfg, CacheLoader loader, InitializationContextImpl ctx) {
      if (loader != null) {
         if (loader instanceof DelegatingCacheLoader)
            loader.init(ctx);
         storesMutex.writeLock().lock();
         try {
            loaders.add(loader);
            storeStatuses.put(loader, new StoreStatus(loader, cfg));
         } finally {
            storesMutex.writeLock().unlock();
         }
      }
   }

   private void initializeBareInstance(Object instance, InitializationContextImpl ctx) {
      // the delegates only propagate init if the underlaying object is a delegate as well.
      // we do this in order to assure the init is only invoked once
      if (instance instanceof CacheWriter) {
         ((CacheWriter) instance).init(ctx);
      } else {
         ((CacheLoader) instance).init(ctx);
      }
   }

   private CacheLoader createCacheLoader(Object instance) {
      return instance instanceof CacheLoader ? (CacheLoader) instance : null;
   }

   private CacheWriter createCacheWriter(Object instance) {
      return instance instanceof CacheWriter ? (CacheWriter) instance : null;
   }

   protected AsyncCacheWriter createAsyncWriter(CacheWriter writer) {
      return (writer instanceof AdvancedCacheWriter) ?
            new AdvancedAsyncCacheWriter(writer) : new AsyncCacheWriter(writer);
   }

   private CacheLoader undelegate(CacheLoader l) {
      return (l instanceof DelegatingCacheLoader) ? ((DelegatingCacheLoader)l).undelegate() : l;
   }

   private CacheWriter undelegate(CacheWriter w) {
      return (w instanceof DelegatingCacheWriter) ? ((DelegatingCacheWriter)w).undelegate() : w;
   }

   private void startWriter(CacheWriter writer, Set<Lifecycle> undelegated) {
      startStore(writer.getClass().getName(), () -> {
         if (writer instanceof DelegatingCacheWriter) {
            CacheWriter actual = undelegate(writer);
            actual.start();
            undelegated.add(actual);
         } else {
            undelegated.add(writer);
         }
         writer.start();

         if (getStoreConfig(writer).purgeOnStartup()) {
            if (!(writer instanceof AdvancedCacheWriter))
               throw new PersistenceException("'purgeOnStartup' can only be set on stores implementing " +
                     "" + AdvancedCacheWriter.class.getName());
            ((AdvancedCacheWriter) writer).clear();
         }
      });
   }

   private void startLoader(CacheLoader loader, Set<Lifecycle> undelegated) {
      CacheLoader delegate = undelegate(loader);
      boolean startInstance = !undelegated.contains(loader);
      boolean startDelegate = loader instanceof DelegatingCacheLoader && !undelegated.contains(delegate);
      startStore(loader.getClass().getName(), () -> {
         if (startInstance)
            loader.start();

         if (startDelegate)
            delegate.start();
      });
   }

   private void startStore(String storeName, Runnable runnable) {
      int connectionAttempts = configuration.persistence().connectionAttempts();
      int connectionInterval = configuration.persistence().connectionInterval();
      for (int i = 0; i < connectionAttempts; i++) {
         try {
            runnable.run();
            return;
         } catch (Exception e) {
            if (i + 1 < connectionAttempts) {
               log.debugf("Exception encountered for store %s on startup attempt %d, retrying ...", storeName, i);
               if (connectionInterval > 0) {
                  try {
                     Thread.sleep(connectionInterval);
                  } catch (InterruptedException ignore) {
                     log.debugf("Thread interrupted for store %s on startup attempt %d, cancelling ...", storeName, i);
                     return;
                  }
               }
            } else {
               throw log.storeStartupAttemptsExceeded(storeName, e);
            }
         }
      }
   }

   private AdvancedCache<Object, Object> getCacheForStateInsertion() {
      List<Flag> flags = new ArrayList<>(Arrays.asList(
            CACHE_MODE_LOCAL, SKIP_OWNERSHIP_CHECK, IGNORE_RETURN_VALUES, SKIP_CACHE_STORE, SKIP_LOCKING,
            SKIP_XSITE_BACKUP));

      boolean hasShared = false;
      storesMutex.readLock().lock();
      try {
         for (CacheWriter w : nonTxWriters) {
            if (getStoreConfig(w).shared()) {
               hasShared = true;
               break;
            }
         }
      } finally {
         storesMutex.readLock().unlock();
      }

      if (hasShared) {
         if (indexShareable())
            flags.add(SKIP_INDEXING);
      } else {
         flags.add(SKIP_INDEXING);
      }

      return cache.wired().withFlags(flags.toArray(new Flag[flags.size()]));
   }

   private boolean indexShareable() {
      return configuration.indexing().indexShareable();
   }

   private long getMaxEntries() {
      if (configuration.memory().isEvictionEnabled()&& configuration.memory().evictionType() == EvictionType.COUNT)
         return configuration.memory().size();
      return Long.MAX_VALUE;
   }

   private void preloadKey(AdvancedCache<Object, Object> cache, Object key, Object value, Metadata metadata) {
      final Transaction transaction = suspendIfNeeded();
      boolean success = false;
      try {
         try {
            beginIfNeeded();
            cache.put(key, value, metadata);
            success = true;
         } catch (Exception e) {
            throw new PersistenceException("Unable to preload!", e);
         } finally {
            commitIfNeeded(success);
         }
      } finally {
         //commitIfNeeded can throw an exception, so we need a try { } finally { }
         resumeIfNeeded(transaction);
      }
   }

   private void resumeIfNeeded(Transaction transaction) {
      if (configuration.transaction().transactionMode().isTransactional() && transactionManager != null &&
            transaction != null) {
         try {
            transactionManager.resume(transaction);
         } catch (Exception e) {
            throw new PersistenceException(e);
         }
      }
   }

   private Transaction suspendIfNeeded() {
      if (configuration.transaction().transactionMode().isTransactional() && transactionManager != null) {
         try {
            return transactionManager.suspend();
         } catch (Exception e) {
            throw new PersistenceException(e);
         }
      }
      return null;
   }

   private void beginIfNeeded() {
      if (configuration.transaction().transactionMode().isTransactional() && transactionManager != null) {
         try {
            transactionManager.begin();
         } catch (Exception e) {
            throw new PersistenceException(e);
         }
      }
   }

   private void commitIfNeeded(boolean success) {
      if (configuration.transaction().transactionMode().isTransactional() && transactionManager != null) {
         try {
            if (success) {
               transactionManager.commit();
            } else {
               transactionManager.rollback();
            }
         } catch (Exception e) {
            throw new PersistenceException(e);
         }
      }
   }

   public StreamingMarshaller getMarshaller() {
      return m;
   }

   private void removeCacheLoader(String storeType, Collection<CacheLoader> collection) {
      for (Iterator<CacheLoader> it = collection.iterator(); it.hasNext(); ) {
         CacheLoader loader = it.next();
         doRemove(it, storeType, loader, undelegate(loader));
      }
   }

   private void removeCacheWriter(String storeType, Collection<? extends CacheWriter> collection) {
      for (Iterator<? extends CacheWriter> it = collection.iterator(); it.hasNext(); ) {
         CacheWriter writer = it.next();
         doRemove(it, storeType, writer, undelegate(writer));
      }
   }

   private void doRemove(Iterator<? extends Lifecycle> it, String storeType, Lifecycle wrapper, Lifecycle actual) {
      if (actual.getClass().getName().equals(storeType)) {
         wrapper.stop();
         if (actual != wrapper) {
            actual.stop();
         }
         it.remove();
         storeStatuses.remove(wrapper);
      }
   }

   private void performOnAllTxStores(Predicate<? super StoreConfiguration> predicate, Consumer<TransactionalCacheWriter> action) {
      storesMutex.readLock().lock();
      try {
         checkStoreAvailability();
         txWriters.stream()
               .filter(writer -> predicate.test(getStoreConfig(writer)))
               .forEach(action);
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   private StoreConfiguration getStoreConfig(Object store) {
      storesMutex.readLock().lock();
      try {
         StoreStatus status = storeStatuses.get(store);
         return status == null ? null : status.config;
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   class StoreStatus {
      final Object store;
      final StoreConfiguration config;
      volatile boolean availability = true;

      StoreStatus(Object store, StoreConfiguration config) {
         this.store = store;
         this.config = config;
      }

      boolean availabilityChanged() {
         boolean oldAvailability = availability;
         try {
            if (store instanceof CacheWriter)
               availability = ((CacheWriter) store).isAvailable();
            else
               availability = ((CacheLoader) store).isAvailable();
         } catch (Throwable t) {
            log.debugf("Error encountered when calling isAvailable on %s: %s", store, t);
            availability = false;
         }
         return oldAvailability != availability;
      }
   }
}
