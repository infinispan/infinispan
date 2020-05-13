package org.infinispan.persistence.manager;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.util.logging.Log.PERSISTENCE;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.cache.impl.InvocationHelper;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.AbstractSegmentedStoreConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.CacheLoaderInterceptor;
import org.infinispan.interceptors.impl.CacheWriterInterceptor;
import org.infinispan.interceptors.impl.TransactionalStoreInterceptor;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.persistence.InitializationContextImpl;
import org.infinispan.persistence.async.AsyncNonBlockingStore;
import org.infinispan.persistence.factory.CacheStoreFactoryRegistry;
import org.infinispan.persistence.spi.LocalOnlyCacheLoader;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.NonBlockingStore.Characteristic;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.spi.StoreUnavailableException;
import org.infinispan.persistence.support.BatchModification;
import org.infinispan.persistence.support.ComposedSegmentedLoadWriteStore;
import org.infinispan.persistence.support.DelegatingNonBlockingStore;
import org.infinispan.persistence.support.NonBlockingStoreAdapter;
import org.infinispan.persistence.support.SegmentPublisherWrapper;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.NonBlockingManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import net.jcip.annotations.GuardedBy;

@Scope(Scopes.NAMED_CACHE)
public class PersistenceManagerImpl implements PersistenceManager {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   @Inject Configuration configuration;
   @Inject GlobalConfiguration globalConfiguration;
   @Inject ComponentRef<AdvancedCache<Object, Object>> cache;
   @Inject CacheStoreFactoryRegistry cacheStoreFactoryRegistry;
   @Inject KeyPartitioner keyPartitioner;
   @Inject TimeService timeService;
   @Inject TransactionManager transactionManager;
   @Inject @ComponentName(KnownComponentNames.PERSISTENCE_MARSHALLER)
   PersistenceMarshaller persistenceMarshaller;
   @Inject ByteBufferFactory byteBufferFactory;
   @Inject CacheNotifier<Object, Object> cacheNotifier;
   @Inject MarshallableEntryFactory marshallableEntryFactory;
   @Inject ComponentRef<CommandsFactory> commandsFactory;
   @ComponentName(KnownComponentNames.NON_BLOCKING_EXECUTOR)
   @Inject Executor nonBlockingExecutor;
   @Inject BlockingManager blockingManager;
   @Inject NonBlockingManager nonBlockingManager;
   @Inject ComponentRef<InvocationHelper> invocationHelper;
   @Inject ComponentRef<InternalExpirationManager<Object, Object>> expirationManager;

   // We use stamped lock since we require releasing locks in threads that may be the same that acquired it
   private final StampedLock lock = new StampedLock();
   // making it volatile as it might change after @Start, so it needs the visibility.
   private volatile boolean enabled;
   private volatile boolean preloaded;
   private volatile boolean clearOnStop;
   private volatile AutoCloseable availabilityTask;
   private volatile String unavailableExceptionMessage;

   private int segmentCount;

   @GuardedBy("lock")
   private final Map<NonBlockingStore<?, ?>, StoreStatus> stores = new LinkedHashMap<>();

   private <K, V> NonBlockingStore<K, V> getStore(Predicate<StoreStatus> predicate) {
      // We almost always will be doing reads, so optimistic should be faster
      // Writes are only done during startup, shutdown and if removing a store
      long stamp = lock.tryOptimisticRead();
      NonBlockingStore<K, V> store = getStoreLocked(predicate);
      if (!lock.validate(stamp)) {
         stamp = acquireReadLock();
         try {
            store = getStoreLocked(predicate);
         } finally {
            releaseReadLock(stamp);
         }
      }
      return store;
   }

   @GuardedBy("lock#readLock")
   private <K, V> NonBlockingStore<K, V> getStoreLocked(Predicate<StoreStatus> predicate) {
      for (Map.Entry<NonBlockingStore<?, ?>, StoreStatus> entry : stores.entrySet()) {
         if (predicate.test(entry.getValue())) {
            return (NonBlockingStore<K, V>) entry.getKey();
         }
      }
      return null;
   }

   @Override
   @Start
   public void start() {
      enabled = configuration.persistence().usingStores();
      if (!enabled)
         return;

      preloaded = false;
      segmentCount = configuration.clustering().hash().numSegments();

      long stamp = lock.writeLock();
      try {
         Completable storeStartup = Flowable.fromIterable(configuration.persistence().stores())
               // We have to ensure stores are started in configured order to ensure the stores map retains that order
               .concatMapSingle(storeConfiguration -> {
                  NonBlockingStore<?, ?> actualStore = storeFromConfiguration(storeConfiguration);
                  NonBlockingStore<?, ?> nonBlockingStore;
                  if (storeConfiguration.async().enabled()) {
                     nonBlockingStore = new AsyncNonBlockingStore<>(actualStore);
                  } else {
                     nonBlockingStore = actualStore;
                  }
                  StoreConfiguration processedConfiguration = cacheStoreFactoryRegistry.processStoreConfiguration(storeConfiguration);
                  InitializationContextImpl ctx =
                        new InitializationContextImpl(processedConfiguration, cache.wired(), keyPartitioner, persistenceMarshaller,
                              timeService, byteBufferFactory, marshallableEntryFactory, nonBlockingExecutor, globalConfiguration, blockingManager);
                  CompletionStage<Void> stage = nonBlockingStore.start(ctx);
                  return Completable.fromCompletionStage(stage)
                        .toSingle(() -> new StoreStatus(nonBlockingStore, processedConfiguration,
                              updateCharacteristics(nonBlockingStore, nonBlockingStore.characteristics(), storeConfiguration)));
               })
               // This relies upon visibility guarnatees of reactive streams for publishing map values
               .doOnNext(status -> stores.put(status.store, status))
               .delay(status -> {
                  if (status.config.purgeOnStartup()) {
                     return Flowable.fromCompletable(Completable.fromCompletionStage(status.store.clear()));
                  }
                  return Flowable.empty();
               })
               .ignoreElements();

         long interval = configuration.persistence().availabilityInterval();
         if (interval > 0) {
            storeStartup = storeStartup.doOnComplete(() ->
               availabilityTask = nonBlockingManager.scheduleWithFixedDelay(this::pollStoreAvailability, interval, interval, MILLISECONDS));
         }

         storeStartup.doOnComplete(() -> lock.unlockWrite(stamp))
               // Blocks here waiting for stores and availability task to start if needed
               .blockingAwait();
      } catch (Throwable t) {
         lock.unlockWrite(stamp);
         throw t;
      }
   }

   private Set<Characteristic> updateCharacteristics(NonBlockingStore store, Set<Characteristic> characteristics,
         StoreConfiguration storeConfiguration) {
      if (storeConfiguration.ignoreModifications()) {
         if (characteristics.contains(Characteristic.WRITE_ONLY)) {
            throw log.storeConfiguredHasBothReadAndWriteOnly(store.getClass().getName(), Characteristic.WRITE_ONLY,
                  Characteristic.READ_ONLY);
         }
         characteristics.add(Characteristic.READ_ONLY);
      }
      if (storeConfiguration.writeOnly()) {
         if (characteristics.contains(Characteristic.READ_ONLY)) {
            throw log.storeConfiguredHasBothReadAndWriteOnly(store.getClass().getName(), Characteristic.READ_ONLY,
                  Characteristic.WRITE_ONLY);
         }
         characteristics.add(Characteristic.WRITE_ONLY);
      }
      return characteristics;
   }

   protected CompletionStage<Void> pollStoreAvailability() {
      if (trace) {
         log.trace("Polling Store availability");
      }
      // This maybe will always be empty - used when all stores are available
      Maybe<NonBlockingStore<Object, Object>> allAvailableMaybe = Maybe.defer(() -> {
         if (unavailableExceptionMessage != null) {
            unavailableExceptionMessage = null;
            return Maybe.fromCompletionStage(cacheNotifier.notifyPersistenceAvailabilityChanged(true)
               .thenApply(CompletableFutures.toNullFunction()));
         }
         return Maybe.empty();
      });
      return Completable.using(this::acquireReadLock,
            ignore -> Flowable.fromIterable(stores.values())
                  .flatMapMaybe(storeStatus -> {
                     CompletionStage<Boolean> availableStage = storeStatus.store.isAvailable();
                     return Maybe.fromCompletionStage(availableStage.thenApply(isAvailable -> {
                        synchronized (storeStatus) {
                           storeStatus.availability = isAvailable;
                        }
                        if (!isAvailable) {
                           return storeStatus.store();
                        }
                        return null;
                     }));
                  }).firstElement()
                  // If it is empty that means all stores were available
                  .switchIfEmpty(allAvailableMaybe)
                  .concatMapCompletable(unavailableStore -> {
                     if (unavailableExceptionMessage == null) {
                        log.debugf("Store %s is unavailable!", unavailableStore);
                        unavailableExceptionMessage = "Store " + unavailableStore + " is unavailable";
                        return Completable.fromCompletionStage(cacheNotifier.notifyPersistenceAvailabilityChanged(false));
                     }
                     return Completable.complete();
                  }),
            this::releaseReadLock)
            .toCompletionStage(null);
   }

   private NonBlockingStore<?, ?> storeFromConfiguration(StoreConfiguration cfg) {
      final Object bareInstance;
      if (cfg.segmented() && cfg instanceof AbstractSegmentedStoreConfiguration) {
         bareInstance = new ComposedSegmentedLoadWriteStore<>((AbstractSegmentedStoreConfiguration) cfg);
      } else {
         bareInstance = cacheStoreFactoryRegistry.createInstance(cfg);
      }
      if (!(bareInstance instanceof NonBlockingStore)) {
         // All prior stores implemented at least Lifecycle
         return new NonBlockingStoreAdapter<>((Lifecycle) bareInstance);
      }
      return (NonBlockingStore<?, ?>) bareInstance;
   }

   @Override
   @Stop
   public void stop() {
      long stamp = lock.writeLock();
      try {
         stopAvailabilityTask();
         Flowable<NonBlockingStore<?, ?>> flowable = Flowable.fromIterable(stores.keySet());
         // If needed, clear the persistent store before stopping
         if (clearOnStop) {
            flowable = flowable
                  .delay(store -> Completable.fromCompletionStage(store.clear()).toFlowable());
         }
         flowable = flowable.delay(store -> Completable.fromCompletionStage(store.stop()).toFlowable());

         // Wait until it completes
         blockingSubscribe(flowable);
         stores.clear();
         preloaded = false;
      } finally {
         lock.unlockWrite(stamp);
      }
   }

   private void stopAvailabilityTask() {
      AutoCloseable taskToClose = availabilityTask;
      if (taskToClose != null) {
         try {
            taskToClose.close();
         } catch (Exception e) {
            log.warn("There was a problem stopping availability task", e);
         }
      }
   }

   // This here solely to document that we are using a blocking method. This is because the start/stop lifecycle
   // methods themselves are blocking but our API is not. This can be removed if lifecycle ever allows for non
   // blocking, but don't hold your breath for it.
   @SuppressWarnings("checkstyle:ForbiddenMethod")
   private void blockingSubscribe(Flowable<?> flowable) {
      flowable.blockingSubscribe();
   }

   @Override
   public boolean isEnabled() {
      return enabled;
   }

   @Override
   public boolean isReadOnly() {
      return getStore(storeStatus -> !storeStatus.characteristics.contains(Characteristic.READ_ONLY)) == null;
   }

   @Override
   public boolean hasWriter() {
      return getStore(storeStatus -> !storeStatus.characteristics.contains(Characteristic.READ_ONLY)) != null;
   }

   @Override
   public boolean isPreloaded() {
      return preloaded;
   }

   @Override
   public CompletionStage<Void> preload() {
      long stamp = acquireReadLock();
      NonBlockingStore<Object, Object> nonBlockingStore = getStoreLocked(storeStatus ->
            storeStatus.config.preload());
      if (nonBlockingStore == null) {
         releaseReadLock(stamp);
         return CompletableFutures.completedNull();
      }
      Publisher<MarshallableEntry<Object, Object>> publisher = nonBlockingStore.publishEntries(
            IntSets.immutableRangeSet(segmentCount), null, true);

      long start = timeService.time();

      final long maxEntries = getMaxEntries();
      final long flags = getFlagsForStateInsertion();
      AdvancedCache<?,?> tmpCache = this.cache.wired().withStorageMediaType();
      DataConversion keyDataConversion = tmpCache.getKeyDataConversion();
      DataConversion valueDataConversion = tmpCache.getValueDataConversion();

      return Flowable.fromPublisher(publisher)
            .doFinally(() -> releaseReadLock(stamp))
            .take(maxEntries)
            .concatMapSingle(me -> preloadKey(flags, me, keyDataConversion, valueDataConversion))
            .count()
            .toCompletionStage()
            .thenAccept(insertAmount -> {
               this.preloaded = insertAmount < maxEntries;
               log.debugf("Preloaded %d keys in %s", insertAmount, Util.prettyPrintTime(timeService.timeDuration(start, MILLISECONDS)));
            });
   }

   private Single<?> preloadKey(long flags, MarshallableEntry me, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      // CallInterceptor will preserve the timestamps if the metadata is an InternalMetadataImpl instance
      InternalMetadataImpl metadata = new InternalMetadataImpl(me.getMetadata(), me.created(), me.lastUsed());
      Object key = keyDataConversion.toStorage(me.getKey());
      Object value = valueDataConversion.toStorage(me.getValue());
      PutKeyValueCommand cmd = commandsFactory.wired().buildPutKeyValueCommand(key, value, keyPartitioner.getSegment(key), metadata, flags);
      cmd.setInternalMetadata(me.getInternalMetadata());

      CompletionStage<Object> stage;
      if (configuration.transaction().transactionMode().isTransactional() && transactionManager != null) {
         final Transaction transaction = suspendIfNeeded();
         CompletionStage<Transaction> putStage;
         try {
            beginIfNeeded();
            putStage = invocationHelper.wired().invokeAsync(cmd, 1)
                  .thenApply(ignore -> {
                     try {
                        return transactionManager.suspend();
                     } catch (SystemException e) {
                        throw new PersistenceException("Unable to preload!", e);
                     }
                  });
         } catch (Exception e) {
            throw new PersistenceException("Unable to preload!", e);
         }
         stage = (CompletionStage) blockingManager.whenCompleteBlocking(putStage, (pendingTransaction, t) -> {
            try {
               transactionManager.resume(pendingTransaction);
               commitIfNeeded(t == null);
            } catch (InvalidTransactionException | SystemException e) {
               throw new PersistenceException("Unable to preload!", e);
            } finally {
               resumeIfNeeded(transaction);
            }
         }, me.getKey());
      } else {
         stage = invocationHelper.wired().invokeAsync(cmd, 1);
      }
      return Maybe.fromCompletionStage(stage)
            .defaultIfEmpty(me);
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

   private void beginIfNeeded() throws SystemException, NotSupportedException {
      if (configuration.transaction().transactionMode().isTransactional() && transactionManager != null) {
         transactionManager.begin();
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

   private long getMaxEntries() {
      long maxCount;
      if (configuration.memory().isEvictionEnabled() && (maxCount = configuration.memory().maxCount()) > 0) {
         return maxCount;
      }
      return Long.MAX_VALUE;
   }

   private long getFlagsForStateInsertion() {
      long flags = FlagBitSets.CACHE_MODE_LOCAL |
            FlagBitSets.SKIP_OWNERSHIP_CHECK |
            FlagBitSets.IGNORE_RETURN_VALUES |
            FlagBitSets.SKIP_CACHE_STORE |
            FlagBitSets.SKIP_LOCKING |
            FlagBitSets.SKIP_XSITE_BACKUP |
            FlagBitSets.IRAC_STATE;

      boolean hasSharedStore  = getStoreLocked(storeStatus -> storeStatus.config.shared()) != null;

      if (!hasSharedStore  || !configuration.indexing().isVolatile()) {
         flags = EnumUtil.mergeBitSets(flags, FlagBitSets.SKIP_INDEXING);
      }

      return flags;
   }

   @Override
   public CompletionStage<Void> disableStore(String storeType) {
      if (!enabled) {
         return CompletableFutures.completedNull();
      }
      boolean stillHasAStore = false;
      AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      long stamp = lock.writeLock();
      try {
         boolean allAvailable = true;
         Iterator<StoreStatus> statusIterator = stores.values().iterator();
         while (statusIterator.hasNext()) {
            StoreStatus status = statusIterator.next();
            NonBlockingStore<?, ?> nonBlockingStore = unwrapStore(status.store());
            if (nonBlockingStore.getClass().getName().equals(storeType) || containedInAdapter(nonBlockingStore, storeType)) {
               statusIterator.remove();
               aggregateCompletionStage.dependsOn(nonBlockingStore.stop()
                     .whenComplete((v, t) -> {
                        if (t != null) {
                           log.warn("There was an error stopping the store", t);
                        }
                     }));
            } else {
               stillHasAStore = true;
               allAvailable = allAvailable && status.availability;
            }
         }

         if (!stillHasAStore) {
            unavailableExceptionMessage = null;
            enabled = false;
            stopAvailabilityTask();
         } else if (allAvailable) {
            unavailableExceptionMessage = null;
         }
      } finally {
         lock.unlockWrite(stamp);
      }

      if (!stillHasAStore) {
         AsyncInterceptorChain chain = cache.wired().getAsyncInterceptorChain();
         AsyncInterceptor loaderInterceptor = chain.findInterceptorExtending(CacheLoaderInterceptor.class);
         if (loaderInterceptor == null) {
            PERSISTENCE.persistenceWithoutCacheLoaderInterceptor();
         } else {
            chain.removeInterceptor(loaderInterceptor.getClass());
         }
         AsyncInterceptor writerInterceptor = chain.findInterceptorExtending(CacheWriterInterceptor.class);
         if (writerInterceptor == null) {
            writerInterceptor = chain.findInterceptorWithClass(TransactionalStoreInterceptor.class);
            if (writerInterceptor == null) {
               PERSISTENCE.persistenceWithoutCacheWriteInterceptor();
            } else {
               chain.removeInterceptor(writerInterceptor.getClass());
            }
         } else {
            chain.removeInterceptor(writerInterceptor.getClass());
         }
      }
      return aggregateCompletionStage.freeze();
   }

   private <K, V> NonBlockingStore<K, V> unwrapStore(NonBlockingStore<K, V> store) {
      if (store instanceof DelegatingNonBlockingStore) {
         return ((DelegatingNonBlockingStore<K, V>) store).delegate();
      }
      return store;
   }

   private Object unwrapOldSPI(NonBlockingStore<?, ?> store) {
      if (store instanceof NonBlockingStoreAdapter) {
         return ((NonBlockingStoreAdapter<?, ?>) store).getActualStore();
      }
      return store;
   }

   private boolean containedInAdapter(NonBlockingStore nonBlockingStore, String adaptedClassName) {
      return nonBlockingStore instanceof NonBlockingStoreAdapter &&
            ((NonBlockingStoreAdapter<?, ?>) nonBlockingStore).getActualStore().getClass().getName().equals(adaptedClassName);
   }

   @Override
   public <T> Set<T> getStores(Class<T> storeClass) {
      long stamp = acquireReadLock();
      try {
         return stores.keySet().stream()
               .map(this::unwrapStore)
               .map(this::unwrapOldSPI)
               .filter(store -> storeClass.isInstance(store))
               .map(store -> (T) store)
               .collect(Collectors.toCollection(HashSet::new));
      } finally {
         releaseReadLock(stamp);
      }
   }

   @Override
   public Collection<String> getStoresAsString() {
      long stamp = acquireReadLock();
      try {
         return stores.keySet().stream()
               .map(store -> store.getClass().getName())
               .collect(Collectors.toCollection(ArrayList::new));
      } finally {
         releaseReadLock(stamp);
      }
   }

   @Override
   public CompletionStage<Void> purgeExpired() {
      long stamp = acquireReadLock();
      try {
         checkStoreAvailability();
         if (trace) {
            log.tracef("Purging entries from stores");
         }
         AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
         for (StoreStatus storeStatus : stores.values()) {
            if (storeStatus.characteristics.contains(Characteristic.EXPIRATION)) {
               Flowable<MarshallableEntry<Object, Object>> flowable = Flowable.fromPublisher(storeStatus.store().purgeExpired());
               Completable completable = flowable.concatMapCompletable(me -> Completable.fromCompletionStage(
                        expirationManager.running().handleInStoreExpirationInternal(me)));
               aggregateCompletionStage.dependsOn(completable.toCompletionStage(null));
            }
         }
         return aggregateCompletionStage.freeze()
               .whenComplete((v, t) -> releaseReadLock(stamp));
      } catch (Throwable t) {
         releaseReadLock(stamp);
         throw t;
      }
   }

   @Override
   public CompletionStage<Void> clearAllStores(Predicate<? super StoreConfiguration> predicate) {
      return Completable.using(
            this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.tracef("Clearing all stores");
               }
               return Flowable.fromIterable(stores.entrySet())
                     .filter(entry ->
                           !entry.getValue().characteristics.contains(Characteristic.READ_ONLY)
                                 && predicate.test(entry.getValue().config))
                     // Let the clear work in parallel across the stores
                     .flatMapCompletable(entry -> Completable.fromCompletionStage(
                           entry.getKey().clear()));
            },
            this::releaseReadLock
      ).toCompletionStage(null);
   }

   @Override
   public CompletionStage<Boolean> deleteFromAllStores(Object key, int segment, Predicate<? super StoreConfiguration> predicate) {
      return Single.using(
            this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.tracef("Deleting entry for key %s from stores", key);
               }
               return Flowable.fromIterable(stores.entrySet())
                     .filter(entry ->
                           !entry.getValue().characteristics.contains(Characteristic.READ_ONLY)
                                 && predicate.test(entry.getValue().config))
                     // Let the delete work in parallel across the stores
                     .flatMapSingle(entry -> Single.fromCompletionStage(
                           entry.getKey().delete(segment, key)))
                     // Can't use any, as we have to reduce to ensure that all stores are updated
                     .reduce(Boolean.FALSE, (removed1, removed2) -> removed1 || removed2);
            },
            this::releaseReadLock
      ).toCompletionStage();
   }

   @Override
   public <K, V> Publisher<MarshallableEntry<K, V>> publishEntries(boolean fetchValue, boolean fetchMetadata) {
      return publishEntries(k -> true, fetchValue, fetchMetadata, k -> true);
   }

   @Override
   public <K, V> Publisher<MarshallableEntry<K, V>> publishEntries(Predicate<? super K> filter, boolean fetchValue,
         boolean fetchMetadata, Predicate<? super StoreConfiguration> predicate) {
      return publishEntries(IntSets.immutableRangeSet(segmentCount), filter, fetchValue, fetchMetadata, predicate);
   }

   @Override
   public <K, V> Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean fetchValue, boolean fetchMetadata, Predicate<? super StoreConfiguration> predicate) {
      return Flowable.using(this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.tracef("Publishing entries for segments %s", segments);
               }
               NonBlockingStore<K, V> nonBlockingStore = getStoreLocked(storeStatus ->
                     storeStatus.characteristics.contains(Characteristic.BULK_READ) && predicate.test(storeStatus.config));
               return nonBlockingStore == null ? Flowable.empty() : nonBlockingStore.publishEntries(segments, filter, fetchValue);
            },
            this::releaseReadLock);
   }

   @Override
   public <K> Publisher<K> publishKeys(Predicate<? super K> filter, Predicate<? super StoreConfiguration> predicate) {
      return publishKeys(IntSets.immutableRangeSet(segmentCount), filter, predicate);
   }

   @Override
   public <K> Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter, Predicate<? super StoreConfiguration> predicate) {
      return Flowable.using(this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.tracef("Publishing keys for segments %s", segments);
               }
               NonBlockingStore<K, ?> nonBlockingStore = getStoreLocked(storeStatus ->
                     storeStatus.characteristics.contains(Characteristic.BULK_READ) && predicate.test(storeStatus.config));
               return nonBlockingStore == null ? Flowable.empty() : nonBlockingStore.publishKeys(segments, filter);
            },
            this::releaseReadLock);
   }

   @Override
   public <K, V> CompletionStage<MarshallableEntry<K, V>> loadFromAllStores(Object key, boolean localInvocation,
         boolean includeStores) {
      return loadFromAllStores(key, keyPartitioner.getSegment(key), localInvocation, includeStores);
   }

   @Override
   public <K, V> CompletionStage<MarshallableEntry<K, V>> loadFromAllStores(Object key, int segment,
         boolean localInvocation, boolean includeStores) {
      return Maybe.using(
            this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.tracef("Loading entry for key %s with segment %d", key, segment);
               }
               return Flowable.fromIterable(stores.entrySet())
                     .filter(entry ->
                           !entry.getValue().characteristics.contains(Characteristic.WRITE_ONLY)
                                 && allowLoad(entry.getValue(), localInvocation, includeStores))
                     // Only do 1 request at a time
                     .concatMapMaybe(entry -> Maybe.fromCompletionStage(
                           PersistenceManagerImpl.<K, V>storeForEntry(entry).load(segment, key)), 1)
                     .firstElement();
            },
            this::releaseReadLock
      ).toCompletionStage(null);
   }

   private boolean allowLoad(StoreStatus storeStatus, boolean localInvocation, boolean includeStores) {
      return (localInvocation || !isLocalOnlyLoader(storeStatus.store)) &&
            (includeStores || storeStatus.characteristics.contains(Characteristic.READ_ONLY) || storeStatus.config.ignoreModifications());
   }

   private boolean isLocalOnlyLoader(NonBlockingStore store) {
      if (store instanceof LocalOnlyCacheLoader) return true;
      NonBlockingStore unwrappedStore;
      if (store instanceof DelegatingNonBlockingStore) {
         unwrappedStore = ((DelegatingNonBlockingStore) store).delegate();
      } else {
         unwrappedStore = store;
      }
      if (unwrappedStore instanceof LocalOnlyCacheLoader) {
         return true;
      }
      if (unwrappedStore instanceof NonBlockingStoreAdapter) {
         return ((NonBlockingStoreAdapter) unwrappedStore).getActualStore() instanceof LocalOnlyCacheLoader;
      }
      return false;
   }

   @Override
   public CompletionStage<Long> size(Predicate<? super StoreConfiguration> predicate) {
      long stamp = acquireReadLock();
      try {
         checkStoreAvailability();
         if (trace) {
            log.tracef("Obtaining size from stores");
         }
         NonBlockingStore<?, ?> nonBlockingStore = getStoreLocked(storeStatus -> storeStatus.characteristics.contains(
               Characteristic.BULK_READ) && predicate.test(storeStatus.config));
         if (nonBlockingStore == null) {
            releaseReadLock(stamp);
            return CompletableFuture.completedFuture(-1L);
         }
         return nonBlockingStore.size(IntSets.immutableRangeSet(segmentCount))
               .whenComplete((ignore, ignoreT) -> releaseReadLock(stamp));
      } catch (Throwable t) {
         releaseReadLock(stamp);
         throw t;
      }
   }

   @Override
   public CompletionStage<Long> size(IntSet segments) {
      long stamp = acquireReadLock();
      try {
         checkStoreAvailability();
         if (trace) {
            log.tracef("Obtaining size from stores for segments %s", segments);
         }
         NonBlockingStore<?, ?> nonBlockingStore = getStoreLocked(storeStatus -> storeStatus.characteristics.contains(
               Characteristic.BULK_READ));
         if (nonBlockingStore == null) {
            releaseReadLock(stamp);
            return CompletableFuture.completedFuture(-1L);
         }
         return nonBlockingStore.size(segments)
               .whenComplete((ignore, ignoreT) -> releaseReadLock(stamp));
      } catch (Throwable t) {
         releaseReadLock(stamp);
         throw t;
      }
   }

   @Override
   public void setClearOnStop(boolean clearOnStop) {
      this.clearOnStop = clearOnStop;
   }

   @Override
   public CompletionStage<Void> writeToAllNonTxStores(MarshallableEntry marshalledEntry, int segment,
         Predicate<? super StoreConfiguration> predicate, long flags) {
      return Completable.using(
            this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.tracef("Writing entry %s for with segment: %d", marshalledEntry, segment);
               }
               return Flowable.fromIterable(stores.entrySet())
                     .filter(entry ->
                           !entry.getValue().characteristics.contains(Characteristic.READ_ONLY)
                                 && predicate.test(entry.getValue().config))
                     // Let the write work in parallel across the stores
                     .flatMapCompletable(entry -> Completable.fromCompletionStage(entry.getKey().write(segment, marshalledEntry)));
            },
            this::releaseReadLock
      ).toCompletionStage(null);
   }

   @Override
   public CompletionStage<Void> prepareAllTxStores(Transaction transaction, BatchModification batchModification, Predicate<? super StoreConfiguration> predicate) throws PersistenceException {
      return Completable.using(
            this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.tracef("Preparing batch for store: %s on transaction %s", batchModification, transaction);
               }
               return Flowable.fromIterable(stores.entrySet())
                     .filter(entry ->
                           !entry.getValue().characteristics.contains(Characteristic.READ_ONLY)
                                 && predicate.test(entry.getValue().config))
                     // Let the prepare work in parallel across the stores
                     .flatMapCompletable(entry -> Completable.fromCompletionStage(entry.getKey().prepareWithModifications(transaction, batchModification)));
            },
            this::releaseReadLock
      ).toCompletionStage(null);
   }

   @Override
   public CompletionStage<Void> commitAllTxStores(Transaction transaction, Predicate<? super StoreConfiguration> predicate) {
      return Completable.using(
            this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.tracef("Committing transaction %s to stores", transaction);
               }
               return Flowable.fromIterable(stores.entrySet())
                     .filter(entry ->
                           !entry.getValue().characteristics.contains(Characteristic.READ_ONLY)
                                 && predicate.test(entry.getValue().config))
                     // Let the commit work in parallel across the stores
                     .flatMapCompletable(entry -> Completable.fromCompletionStage(entry.getKey().commit(transaction)));
            },
            this::releaseReadLock
      ).toCompletionStage(null);
   }

   @Override
   public CompletionStage<Void> rollbackAllTxStores(Transaction transaction, Predicate<? super StoreConfiguration> predicate) {
      return Completable.using(
            this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.tracef("Rolling back transaction %s for stores", transaction);
               }
               return Flowable.fromIterable(stores.entrySet())
                     .filter(entry ->
                           !entry.getValue().characteristics.contains(Characteristic.READ_ONLY)
                                 && predicate.test(entry.getValue().config))
                     // Let the rollback work in parallel across the stores
                     .flatMapCompletable(entry -> Completable.fromCompletionStage(entry.getKey().commit(transaction)));
            },
            this::releaseReadLock
      ).toCompletionStage(null);
   }

   @Override
   public <K, V> CompletionStage<Void> writeBatchToAllNonTxStores(Iterable<MarshallableEntry<K, V>> entries, Predicate<? super StoreConfiguration> predicate, long flags) {
      Flowable<NonBlockingStore.SegmentedPublisher<MarshallableEntry<K, V>>> flowable = Flowable.fromIterable(entries)
            .groupBy(me -> keyPartitioner.getSegment(me.getKey()))
            .map(SegmentPublisherWrapper::new);
      return Completable.using(
            this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.trace("Writing batch to stores");
               }
               return Flowable.fromIterable(stores.entrySet())
                     .filter(entry ->
                           !entry.getValue().characteristics.contains(Characteristic.READ_ONLY)
                                 && predicate.test(entry.getValue().config))
                     // Let the rollback work in parallel across the stores
                     .flatMapCompletable(entry -> Completable.fromCompletionStage(
                           PersistenceManagerImpl.<K, V>storeForEntry(entry).bulkWrite(segmentCount, flowable)));
            },
            this::releaseReadLock
      ).toCompletionStage(null);
   }

   @Override
   public CompletionStage<Void> deleteBatchFromAllNonTxStores(Iterable<Object> keys, Predicate<? super StoreConfiguration> predicate, long flags) {
      Flowable<NonBlockingStore.SegmentedPublisher<Object>> flowable = Flowable.fromIterable(keys)
            .groupBy(keyPartitioner::getSegment)
            .map(SegmentPublisherWrapper::new);
      return Completable.using(
            this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.trace("Deleting batch of entries from stores");
               }
               return Flowable.fromIterable(stores.entrySet())
                     .filter(entry ->
                           !entry.getValue().characteristics.contains(Characteristic.READ_ONLY)
                                 && predicate.test(entry.getValue().config))
                     // Let the delete batch work in parallel across the stores
                     .flatMapCompletable(entry -> Completable.fromCompletionStage(
                           entry.getKey().bulkDelete(segmentCount, flowable)));
            },
            this::releaseReadLock
      ).toCompletionStage(null);
   }

   @Override
   public boolean isAvailable() {
      return unavailableExceptionMessage == null;
   }

   @Override
   public CompletionStage<Boolean> addSegments(IntSet segments) {
      return Single.using(
            this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.tracef("Adding segments %s to stores", segments);
               }
               int size = stores.entrySet().size();
               return Flowable.fromIterable(stores.entrySet())
                     .filter(entry -> entry.getValue().characteristics.contains(Characteristic.SEGMENTABLE))
                     .delay(entry -> Completable.fromCompletionStage(entry.getKey().addSegments(segments)).toFlowable())
                     .count()
                     .map(count -> size == count);
            },
            this::releaseReadLock
      ).toCompletionStage();
   }

   @Override
   public CompletionStage<Boolean> removeSegments(IntSet segments) {
      return Single.using(
            this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.tracef("Removing segments %s from stores", segments);
               }
               int size = stores.entrySet().size();
               return Flowable.fromIterable(stores.entrySet())
                     .filter(entry -> entry.getValue().characteristics.contains(Characteristic.SEGMENTABLE))
                     .delay(entry -> Completable.fromCompletionStage(entry.getKey().removeSegments(segments)).toFlowable())
                     .count()
                     .map(count -> size == count);
            },
            this::releaseReadLock
      ).toCompletionStage();
   }

   static <K, V> NonBlockingStore<K, V> storeForEntry(Map.Entry<NonBlockingStore<?, ?>, StoreStatus> entry) {
      return (NonBlockingStore) entry.getKey();
   }

   public <K, V> List<NonBlockingStore<K, V>> getAllStores(Predicate<Set<Characteristic>> predicate) {
      long stamp = acquireReadLock();
      try {
         return stores.values().stream()
               .filter(storeStatus -> predicate.test(storeStatus.characteristics))
               .map(storeStatus -> (NonBlockingStore<K, V>) storeStatus.store)
               .collect(Collectors.toCollection(ArrayList::new));
      } finally {
         releaseReadLock(stamp);
      }
   }

   /**
    * Method must be here for augmentation to tell blockhound this method is okay to block
    */
   private long acquireReadLock() {
      return lock.readLock();
   }

   /**
    * Opposite of acquireReadLock here for symmetry
    */
   private void releaseReadLock(long stamp) {
      lock.unlockRead(stamp);
   }

   private void checkStoreAvailability() {
      if (!enabled) return;

      String message = unavailableExceptionMessage;
      if (message != null) {
         throw new StoreUnavailableException(message);
      }
   }

   static class StoreStatus {
      final NonBlockingStore<?, ?> store;
      final StoreConfiguration config;
      final Set<Characteristic> characteristics;
      @GuardedBy("this")
      boolean availability = true;

      StoreStatus(NonBlockingStore<?, ?> store, StoreConfiguration config, Set<Characteristic> characteristics) {
         this.store = store;
         this.config = config;
         this.characteristics = characteristics;
      }

      <K, V> NonBlockingStore<K, V> store() {
         return (NonBlockingStore) store;
      }
   }

   boolean anyLocksHeld() {
      return lock.isReadLocked() || lock.isWriteLocked();
   }
}
