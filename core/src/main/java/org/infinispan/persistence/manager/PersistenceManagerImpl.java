package org.infinispan.persistence.manager;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.util.logging.Log.CONFIG;
import static org.infinispan.util.logging.Log.PERSISTENCE;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.factories.InterceptorChainFactory;
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
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.persistence.InitializationContextImpl;
import org.infinispan.persistence.async.AsyncNonBlockingStore;
import org.infinispan.persistence.internal.PersistenceUtil;
import org.infinispan.persistence.spi.LocalOnlyCacheLoader;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.NonBlockingStore.Characteristic;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.spi.StoreUnavailableException;
import org.infinispan.persistence.support.DelegatingNonBlockingStore;
import org.infinispan.persistence.support.NonBlockingStoreAdapter;
import org.infinispan.persistence.support.SegmentPublisherWrapper;
import org.infinispan.persistence.support.SingleSegmentPublisher;
import org.infinispan.transaction.impl.AbstractCacheTransaction;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.NonBlockingManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.functions.Function;
import net.jcip.annotations.GuardedBy;

@Scope(Scopes.NAMED_CACHE)
public class PersistenceManagerImpl implements PersistenceManager {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @Inject Configuration configuration;
   @Inject GlobalConfiguration globalConfiguration;
   @Inject ComponentRef<AdvancedCache<Object, Object>> cache;
   @Inject KeyPartitioner keyPartitioner;
   @Inject TimeService timeService;
   @Inject @ComponentName(KnownComponentNames.PERSISTENCE_MARSHALLER)
   PersistenceMarshaller persistenceMarshaller;
   @Inject ByteBufferFactory byteBufferFactory;
   @Inject CacheNotifier<Object, Object> cacheNotifier;
   @Inject InternalEntryFactory internalEntryFactory;
   @Inject MarshallableEntryFactory<?, ?> marshallableEntryFactory;
   @ComponentName(KnownComponentNames.NON_BLOCKING_EXECUTOR)
   @Inject Executor nonBlockingExecutor;
   @Inject BlockingManager blockingManager;
   @Inject NonBlockingManager nonBlockingManager;
   @Inject ComponentRef<InternalExpirationManager<Object, Object>> expirationManager;
   @Inject DistributionManager distributionManager;
   @Inject InterceptorChainFactory interceptorChainFactory;

   // We use stamped lock since we require releasing locks in threads that may be the same that acquired it
   private final StampedLock lock = new StampedLock();
   // making it volatile as it might change after @Start, so it needs the visibility.
   private volatile boolean enabled;
   private volatile boolean clearOnStop;
   private volatile AutoCloseable availabilityTask;
   private volatile String unavailableExceptionMessage;

   // Writes to an invalidation cache skip the shared check
   private boolean isInvalidationCache;
   private boolean allSegmentedOrShared;

   private int segmentCount;

   @GuardedBy("lock")
   private List<StoreStatus> stores = null;

   private final List<StoreChangeListener> listeners = new CopyOnWriteArrayList<>();

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
      if (stores == null) {
         return null;
      }
      for (StoreStatus storeStatus : stores) {
         if (predicate.test(storeStatus)) {
            return storeStatus.store();
         }
      }
      return null;
   }

   @GuardedBy("lock#readLock")
   private StoreStatus getStoreStatusLocked(Predicate<? super StoreStatus> predicate) {
      for (StoreStatus storeStatus : stores) {
         if (predicate.test(storeStatus)) {
            return storeStatus;
         }
      }
      return null;
   }


   @Override
   @Start
   public void start() {
      enabled = configuration.persistence().usingStores();
      segmentCount = configuration.clustering().hash().numSegments();

      isInvalidationCache = configuration.clustering().cacheMode().isInvalidation();
      if (!enabled)
         return;
      // Blocks here waiting for stores and availability task to start if needed
      Completable.using(this::acquireWriteLock,
                  __ -> startManagerAndStores(configuration.persistence().stores()),
                  this::releaseWriteLock)
            .blockingAwait();
   }

   @GuardedBy("lock#writeLock")
   private Completable startManagerAndStores(Collection<StoreConfiguration> storeConfigurations) {
      if (storeConfigurations.isEmpty()) {
         throw new IllegalArgumentException("Store configurations require at least one configuration");
      }
      enabled = true;
      if (stores == null) {
         stores = new ArrayList<>(storeConfigurations.size());
      }
      Completable storeStartup = startStoresOnly(storeConfigurations);

      long interval = configuration.persistence().availabilityInterval();
      if (interval > 0 && availabilityTask == null) {
         storeStartup = storeStartup.doOnComplete(() ->
               availabilityTask = nonBlockingManager.scheduleWithFixedDelay(this::pollStoreAvailability, interval,
                     interval, MILLISECONDS, t -> !(t instanceof Error)));
      }
      return storeStartup.doOnComplete(() -> {
         boolean hasMaxIdle = configuration.expiration().maxIdle() > 0;
         boolean hasLifespan = configuration.expiration().lifespan() > 0;

         if (hasLifespan || hasMaxIdle) {
            stores.stream().forEach(status -> {
               // If a store is not writeable, then expiration works fine as it only expires in memory, thus refreshing
               // the value that can be read from the store
               if (status.hasCharacteristic(Characteristic.READ_ONLY)) {
                  return;
               }
               if (hasMaxIdle) {
                  // Max idle is not currently supported with stores, it sorta works with passivation though
                  if (!configuration.persistence().passivation()) {
                     throw CONFIG.maxIdleNotAllowedWithoutPassivation();
                  }
                  CONFIG.maxIdleNotTestedWithPassivation();
               }
               if (!status.hasCharacteristic(Characteristic.EXPIRATION)) {
                  throw CONFIG.expirationNotAllowedWhenStoreDoesNotSupport(status.store.getClass().getName());
               }
            });
         }

         allSegmentedOrShared = allStoresSegmentedOrShared();
      });
   }

   private Completable startStoresOnly(Iterable<StoreConfiguration> storeConfigurations) {
      return Flowable.fromIterable(storeConfigurations)
            // We have to ensure stores are started in configured order to ensure the stores map retains that order
            .concatMapSingle(storeConfiguration -> {
               NonBlockingStore<?, ?> actualStore = PersistenceUtil.storeFromConfiguration(storeConfiguration);
               NonBlockingStore<?, ?> nonBlockingStore;
               if (storeConfiguration.async().enabled()) {
                  nonBlockingStore = new AsyncNonBlockingStore<>(actualStore);
               } else {
                  nonBlockingStore = actualStore;
               }
               InitializationContextImpl ctx =
                     new InitializationContextImpl(storeConfiguration, cache.wired(), keyPartitioner, persistenceMarshaller,
                           timeService, byteBufferFactory, marshallableEntryFactory, nonBlockingExecutor,
                           globalConfiguration, blockingManager, nonBlockingManager);
               CompletionStage<Void> stage = nonBlockingStore.start(ctx).whenComplete((ignore, t) -> {
                  // On exception, just put a status with only the store - this way we can still invoke stop on it later
                  if (t != null) {
                     stores.add(new StoreStatus(nonBlockingStore, null, null));
                  }
               });
               return Completable.fromCompletionStage(stage)
                     .toSingle(() -> new StoreStatus(nonBlockingStore, storeConfiguration,
                           updateCharacteristics(nonBlockingStore, nonBlockingStore.characteristics(), storeConfiguration)));
            })
            // This relies upon visibility guarantees of reactive streams for publishing map values
            .doOnNext(stores::add)
            .delay(status -> {
               if (status.config.purgeOnStartup()) {
                  return Flowable.fromCompletable(Completable.fromCompletionStage(status.store.clear()));
               }
               return Flowable.empty();
            }).ignoreElements();
   }

   @GuardedBy("lock")
   private boolean allStoresSegmentedOrShared() {
      return getStoreLocked(storeStatus -> !storeStatus.hasCharacteristic(Characteristic.SEGMENTABLE) ||
            !storeStatus.hasCharacteristic(Characteristic.SHAREABLE)) != null;
   }

   private Set<Characteristic> updateCharacteristics(NonBlockingStore<?, ?> store, Set<Characteristic> characteristics,
         StoreConfiguration storeConfiguration) {
      if (storeConfiguration.ignoreModifications()) {
         if (characteristics.contains(Characteristic.WRITE_ONLY)) {
            throw log.storeConfiguredHasBothReadAndWriteOnly(store.getClass().getName(), Characteristic.WRITE_ONLY,
                  Characteristic.READ_ONLY);
         }
         characteristics.add(Characteristic.READ_ONLY);
         characteristics.remove(Characteristic.TRANSACTIONAL);
      }
      if (storeConfiguration.writeOnly()) {
         if (characteristics.contains(Characteristic.READ_ONLY)) {
            throw log.storeConfiguredHasBothReadAndWriteOnly(store.getClass().getName(), Characteristic.READ_ONLY,
                  Characteristic.WRITE_ONLY);
         }
         characteristics.add(Characteristic.WRITE_ONLY);
         characteristics.remove(Characteristic.BULK_READ);
      }
      if (storeConfiguration.segmented()) {
         if (!characteristics.contains(Characteristic.SEGMENTABLE)) {
            throw log.storeConfiguredSegmentedButCharacteristicNotPresent(store.getClass().getName());
         }
      } else {
         characteristics.remove(Characteristic.SEGMENTABLE);
      }
      if (storeConfiguration.transactional()) {
         if (!characteristics.contains(Characteristic.TRANSACTIONAL)) {
            throw log.storeConfiguredTransactionalButCharacteristicNotPresent(store.getClass().getName());
         }
      } else {
         characteristics.remove(Characteristic.TRANSACTIONAL);
      }
      if (storeConfiguration.shared()) {
         if (!characteristics.contains(Characteristic.SHAREABLE)) {
            throw log.storeConfiguredSharedButCharacteristicNotPresent(store.getClass().getName());
         }
      } else {
         characteristics.remove(Characteristic.SHAREABLE);
      }
      return characteristics;
   }

   /**
    * Polls the availability of all configured stores.
    * <p>
    * If a store is found to be unavailable all future requests to this manager will throw a
    * {@link StoreUnavailableException}, until the stores are all available again.
    * <p>
    * Note that this method should not be called until the previous invocation's stage completed.
    * {@link NonBlockingManager#scheduleWithFixedDelay(java.util.function.Supplier, long, long, java.util.concurrent.TimeUnit)}
    * guarantees that.
    *
    * @return stage that completes when all store availability checks are done
    */
   protected CompletionStage<Void> pollStoreAvailability() {
      if (log.isTraceEnabled()) {
         log.trace("Polling Store availability");
      }
      AtomicReference<NonBlockingStore<?, ?>> firstUnavailableStore = new AtomicReference<>();
      long stamp = acquireReadLock();
      boolean release = true;
      try {
         AggregateCompletionStage<Void> stageBuilder = CompletionStages.aggregateCompletionStage();
         for (StoreStatus storeStatus : stores) {
            CompletionStage<Boolean> availableStage;
            try {
               availableStage = storeStatus.store.isAvailable();
            } catch (Throwable t) {
               log.storeIsAvailableCheckThrewException(t, storeStatus.store.getClass().getName());
               availableStage = CompletableFutures.booleanStage(false);
            }

            availableStage = availableStage.exceptionally(throwable -> {
               log.storeIsAvailableCompletedExceptionally(throwable, storeStatus.store.getClass().getName());
               return false;
            });

            stageBuilder.dependsOn(availableStage.thenCompose(isAvailable -> {
               storeStatus.availability = isAvailable;
               if (!isAvailable) {
                  // Update persistence availability as soon as we know one store is unavailable
                  firstUnavailableStore.compareAndSet(null, storeStatus.store);
                  return updatePersistenceAvailability(storeStatus.store);
               }
               return CompletableFutures.completedNull();
            }));
         }
         CompletionStage<Void> stage = stageBuilder.freeze();
         if (CompletionStages.isCompletedSuccessfully(stage)) {
            return updatePersistenceAvailability(firstUnavailableStore.get());
         } else {
            release = false;
            return stage.thenCompose(__ -> updatePersistenceAvailability(firstUnavailableStore.get()))
                        .whenComplete((e, throwable) -> releaseReadLock(stamp));
         }
      } finally {
         if (release) {
            releaseReadLock(stamp);
         }
      }
   }

   private CompletionStage<Void> updatePersistenceAvailability(NonBlockingStore<?, ?> unavailableStore) {
      // No locking needed: there is only one availability check task running at any given time
      if (unavailableStore != null) {
         if (unavailableExceptionMessage == null) {
            log.persistenceUnavailable(unavailableStore.getClass().getName());
            unavailableExceptionMessage = "Store " + unavailableStore + " is unavailable";
            return cacheNotifier.notifyPersistenceAvailabilityChanged(false);
         }
      } else {
         // All stores are available
         if (unavailableExceptionMessage != null) {
            log.persistenceAvailable();
            unavailableExceptionMessage = null;
            return cacheNotifier.notifyPersistenceAvailabilityChanged(true);
         }
      }
      return CompletableFutures.completedNull();
   }

   @Override
   @Stop
   public void stop() {
      AggregateCompletionStage<Void> allStage = CompletionStages.aggregateCompletionStage();
      long stamp = acquireWriteLock();
      try {
         stopAvailabilityTask();

         if (stores == null)
            return;

         for (StoreStatus storeStatus : stores) {
            NonBlockingStore<Object, Object> store = storeStatus.store();
            CompletionStage<Void> storeStage;
            if (clearOnStop && !storeStatus.hasCharacteristic(Characteristic.READ_ONLY)) {
               // Clear the persistent store before stopping
               storeStage = store.clear().thenCompose(__ -> store.stop());
            } else {
               storeStage = store.stop();
            }
            allStage.dependsOn(storeStage);
         }
         stores = null;
      } finally {
         releaseWriteLock(stamp);
      }
      // Wait until it completes
      CompletionStages.join(allStage.freeze());
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

   @Override
   public boolean isEnabled() {
      return enabled;
   }

   @Override
   public boolean isReadOnly() {
      return getStore(storeStatus -> !storeStatus.hasCharacteristic(Characteristic.READ_ONLY)) == null;
   }

   @Override
   public boolean hasWriter() {
      return getStore(storeStatus -> !storeStatus.hasCharacteristic(Characteristic.READ_ONLY)) != null;
   }

   @Override
   public boolean hasStore(Predicate<StoreConfiguration> test) {
      return getStore(storeStatus -> test.test(storeStatus.config)) != null;
   }

   @Override
   public Flowable<MarshallableEntry<Object, Object>> preloadPublisher() {
      long stamp = acquireReadLock();
      NonBlockingStore<Object, Object> nonBlockingStore = getStoreLocked(status -> status.config.preload());
      if (nonBlockingStore == null) {
         releaseReadLock(stamp);
         return Flowable.empty();
      }
      Publisher<MarshallableEntry<Object, Object>> publisher = nonBlockingStore.publishEntries(
            IntSets.immutableRangeSet(segmentCount), null, true);

      return Flowable.fromPublisher(publisher)
                     .doFinally(() -> releaseReadLock(stamp));
   }

   @Override
   public void addStoreListener(StoreChangeListener listener) {
      listeners.add(listener);
   }

   @Override
   public void removeStoreListener(StoreChangeListener listener) {
      listeners.remove(listener);
   }

   @Override
   public CompletionStage<Void> addStore(StoreConfiguration storeConfiguration) {
      return Single.fromCompletionStage(cache.wired().sizeAsync())
            .doOnSuccess(l -> {
               if (l > 0) throw log.cannotAddStore(cache.wired().getName());
            })
            .concatMapCompletable(v -> Completable.using(this::acquireWriteLock, lock ->
                        startManagerAndStores(singletonList(storeConfiguration))
                              .doOnComplete(() -> {
                                 AsyncInterceptorChain chain = cache.wired().getAsyncInterceptorChain();
                                 interceptorChainFactory.addPersistenceInterceptors(chain, configuration, singletonList(storeConfiguration));
                                 listeners.forEach(l -> l.storeChanged(createStatus()));
                              })
                  , this::releaseWriteLock))
            .toCompletionStage(null);
   }

   private PersistenceStatus createStatus() {
      boolean usingSharedAsync = false;
      boolean usingSegments = false;
      boolean usingAsync = false;
      boolean usingReadOnly = false;
      boolean usingTransactionalStore = false;
      for (StoreStatus storeStatus : stores) {
         if (storeStatus.config.async().enabled()) {
            usingSharedAsync |= storeStatus.config.shared();
            usingAsync = true;
         }
         if (storeStatus.config.segmented()) {
            usingSegments = true;
         }
         if (storeStatus.config.ignoreModifications()) {
            usingReadOnly = true;
         }
         if (storeStatus.config.transactional()) {
            usingTransactionalStore = true;
         }
      }
      return new PersistenceStatus(enabled, usingSegments, usingAsync, usingSharedAsync,
            usingReadOnly, usingTransactionalStore);
   }

   @Override
   public CompletionStage<Void> disableStore(String storeType) {
      boolean stillHasAStore = false;
      AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      long stamp = lock.writeLock();
      try {
         if (!checkStoreAvailability()) {
            return CompletableFutures.completedNull();
         }
         boolean allAvailable = true;
         Iterator<StoreStatus> statusIterator = stores.iterator();
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
         allSegmentedOrShared = allStoresSegmentedOrShared();
         listeners.forEach(l -> l.storeChanged(createStatus()));

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
      } finally {
         lock.unlockWrite(stamp);
      }
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

   private boolean containedInAdapter(NonBlockingStore<?, ?> nonBlockingStore, String adaptedClassName) {
      return nonBlockingStore instanceof NonBlockingStoreAdapter &&
            ((NonBlockingStoreAdapter<?, ?>) nonBlockingStore).getActualStore().getClass().getName().equals(adaptedClassName);
   }

   @Override
   public <T> Set<T> getStores(Class<T> storeClass) {
      long stamp = acquireReadLock();
      try {
         if (!checkStoreAvailability()) {
            return Collections.emptySet();
         }
         return stores.stream()
               .map(StoreStatus::store)
               .map(this::unwrapStore)
               .map(this::unwrapOldSPI)
               .filter(storeClass::isInstance)
               .map(storeClass::cast)
               .collect(Collectors.toCollection(HashSet::new));
      } finally {
         releaseReadLock(stamp);
      }
   }

   @Override
   public Collection<String> getStoresAsString() {
      long stamp = acquireReadLock();
      try {
         if (!checkStoreAvailability()) {
            return Collections.emptyList();
         }
         return stores.stream()
               .map(StoreStatus::store)
               .map(this::unwrapStore)
               .map(this::unwrapOldSPI)
               .map(c -> c.getClass().getName())
               .collect(Collectors.toCollection(ArrayList::new));
      } finally {
         releaseReadLock(stamp);
      }
   }

   @Override
   public CompletionStage<Void> purgeExpired() {
      long stamp = acquireReadLock();
      try {
         if (!checkStoreAvailability()) {
            releaseReadLock(stamp);
            return CompletableFutures.completedNull();
         }
         if (log.isTraceEnabled()) {
            log.tracef("Purging entries from stores on cache %s", cache.getName());
         }
         AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
         for (StoreStatus storeStatus : stores) {
            if (storeStatus.hasCharacteristic(Characteristic.EXPIRATION)) {
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
      long stamp = acquireReadLock();
      boolean release = true;
      try {
         if (!checkStoreAvailability()) {
            return CompletableFutures.completedNull();
         }
         if (log.isTraceEnabled()) {
            log.tracef("Clearing all stores");
         }
         // Let the clear work in parallel across the stores
         AggregateCompletionStage<Void> stageBuilder = CompletionStages.aggregateCompletionStage();
         for (StoreStatus storeStatus : stores) {
            if (!storeStatus.hasCharacteristic(Characteristic.READ_ONLY)
                  && predicate.test(storeStatus.config)) {
               stageBuilder.dependsOn(storeStatus.store.clear());
            }
         }
         CompletionStage<Void> stage = stageBuilder.freeze();
         if (CompletionStages.isCompletedSuccessfully(stage)) {
            return stage;
         } else {
            release = false;
            return stage.whenComplete((e, throwable) -> releaseReadLock(stamp));
         }
      } finally {
         if (release) {
            releaseReadLock(stamp);
         }
      }
   }

   @Override
   public CompletionStage<Boolean> deleteFromAllStores(Object key, int segment, Predicate<? super StoreConfiguration> predicate) {
      long stamp = acquireReadLock();
      boolean release = true;
      try {
         if (!checkStoreAvailability()) {
            return CompletableFutures.completedFalse();
         }
         if (log.isTraceEnabled()) {
            log.tracef("Deleting entry for key %s from stores", key);
         }

         if (stores.isEmpty())
            return CompletableFutures.completedFalse();

         // Let the write work in parallel across the stores
         AtomicBoolean removedAny = new AtomicBoolean();
         AggregateCompletionStage<AtomicBoolean> stageBuilder = CompletionStages.aggregateCompletionStage(removedAny);
         for (StoreStatus storeStatus : stores) {
            if (!storeStatus.hasCharacteristic(Characteristic.READ_ONLY)
                  && predicate.test(storeStatus.config)) {
               stageBuilder.dependsOn(storeStatus.store.delete(segment, key)
                     .thenAccept(removed -> {
                        // If a store doesn't say, pretend it was removed
                        if (removed == null || removed) {
                           removedAny.set(true);
                        }
                     }));
            }
         }
         CompletionStage<AtomicBoolean> stage = stageBuilder.freeze();
         if (CompletionStages.isCompletedSuccessfully(stage)) {
            return CompletableFutures.booleanStage(removedAny.get());
         } else {
            release = false;
            return stage.handle((removed, throwable) -> {
               releaseReadLock(stamp);
               if (throwable != null) {
                  throw CompletableFutures.asCompletionException(throwable);
               }

               return removed.get();
            });
         }
      } finally {
         if (release) {
            releaseReadLock(stamp);
         }
      }
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
   public <K, V> Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter,
         boolean fetchValue, boolean fetchMetadata, Predicate<? super StoreConfiguration> predicate) {
      return Flowable.using(this::acquireReadLock,
            ignore -> {
               if (!checkStoreAvailability()) {
                  return Flowable.empty();
               }
               if (log.isTraceEnabled()) {
                  log.tracef("Publishing entries for segments %s", segments);
               }
               for (StoreStatus storeStatus : stores) {
                  Set<Characteristic> characteristics = storeStatus.characteristics;
                  if (characteristics.contains(Characteristic.BULK_READ) && predicate.test(storeStatus.config)) {
                     Predicate<? super K> filterToUse;
                     if (!characteristics.contains(Characteristic.SEGMENTABLE) &&
                           !segments.containsAll(IntSets.immutableRangeSet(segmentCount))) {
                        filterToUse = PersistenceUtil.combinePredicate(segments, keyPartitioner, filter);
                     } else {
                        filterToUse = filter;
                     }
                     return storeStatus.<K, V>store().publishEntries(segments, filterToUse, fetchValue);
                  }
               }
               return Flowable.empty();
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
                               if (!checkStoreAvailability()) {
                                  return Flowable.empty();
                               }
                               if (log.isTraceEnabled()) {
                                  log.tracef("Publishing keys for segments %s", segments);
                               }
                               for (StoreStatus storeStatus : stores) {
                                  Set<Characteristic> characteristics = storeStatus.characteristics;
                                  if (characteristics.contains(Characteristic.BULK_READ) &&
                                        predicate.test(storeStatus.config)) {
                                     Predicate<? super K> filterToUse;
                                     if (!characteristics.contains(Characteristic.SEGMENTABLE) &&
                                           !segments.containsAll(IntSets.immutableRangeSet(segmentCount))) {
                                        filterToUse =
                                              PersistenceUtil.combinePredicate(segments, keyPartitioner, filter);
                                     } else {
                                        filterToUse = filter;
                                     }
                                     return storeStatus.<K, Object>store().publishKeys(segments, filterToUse);
                                  }
                               }
                               return Flowable.empty();
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
      long stamp = acquireReadLock();
      boolean release = true;
      try {
         if (!checkStoreAvailability()) {
            return CompletableFutures.completedNull();
         }
         if (log.isTraceEnabled()) {
            log.tracef("Loading entry for key %s with segment %d", key, segment);
         }
         Iterator<StoreStatus> iterator = stores.iterator();
         CompletionStage<MarshallableEntry<K, V>> stage =
               loadFromStoresIterator(key, segment, iterator, localInvocation, includeStores);
         if (CompletionStages.isCompletedSuccessfully(stage)) {
            return stage;
         } else {
            release = false;
            return stage.whenComplete((e, throwable) -> releaseReadLock(stamp));
         }
      } finally {
         if (release) {
            releaseReadLock(stamp);
         }
      }
   }

   private <K, V> CompletionStage<MarshallableEntry<K, V>> loadFromStoresIterator(Object key, int segment,
                                                                                  Iterator<StoreStatus> iterator,
                                                                                  boolean localInvocation,
                                                                                  boolean includeStores) {
      while (iterator.hasNext()) {
         StoreStatus storeStatus = iterator.next();
         NonBlockingStore<K, V> store = storeStatus.store();
         if (!allowLoad(storeStatus, localInvocation, includeStores)) {
            continue;
         }
         CompletionStage<MarshallableEntry<K, V>> loadStage = store.load(segmentOrZero(storeStatus, segment), key);
         return loadStage.thenCompose(e -> {
            if (e != null) {
               return CompletableFuture.completedFuture(e);
            } else {
               return loadFromStoresIterator(key, segment, iterator, localInvocation, includeStores);
            }
         });
      }
      return CompletableFutures.completedNull();
   }

   private boolean allowLoad(StoreStatus storeStatus, boolean localInvocation, boolean includeStores) {
      return !storeStatus.hasCharacteristic(Characteristic.WRITE_ONLY) &&
            (localInvocation || !isLocalOnlyLoader(storeStatus.store)) &&
            (includeStores || storeStatus.hasCharacteristic(Characteristic.READ_ONLY) ||
                  storeStatus.config.ignoreModifications());
   }

   private boolean isLocalOnlyLoader(NonBlockingStore<?, ?> store) {
      if (store instanceof LocalOnlyCacheLoader) return true;
      NonBlockingStore<?, ?> unwrappedStore;
      if (store instanceof DelegatingNonBlockingStore) {
         unwrappedStore = ((DelegatingNonBlockingStore<?, ?>) store).delegate();
      } else {
         unwrappedStore = store;
      }
      if (unwrappedStore instanceof LocalOnlyCacheLoader) {
         return true;
      }
      if (unwrappedStore instanceof NonBlockingStoreAdapter) {
         return ((NonBlockingStoreAdapter<?, ?>) unwrappedStore).getActualStore() instanceof LocalOnlyCacheLoader;
      }
      return false;
   }

   @Override
   public CompletionStage<Long> approximateSize(Predicate<? super StoreConfiguration> predicate, IntSet segments) {
      if (!isEnabled()) {
         return NonBlockingStore.SIZE_UNAVAILABLE_FUTURE;
      }
      long stamp = acquireReadLock();
      try {
         if (!isAvailable()) {
            releaseReadLock(stamp);
            return NonBlockingStore.SIZE_UNAVAILABLE_FUTURE;
         }

         if (stores == null) {
            throw new IllegalLifecycleStateException();
         }

         // Ignore stores without BULK_READ, they don't implement approximateSize()
         StoreStatus firstStoreStatus = getStoreStatusLocked(storeStatus ->
               storeStatus.hasCharacteristic(Characteristic.BULK_READ) &&
                     predicate.test(storeStatus.config));

         if (firstStoreStatus == null) {
            releaseReadLock(stamp);
            return NonBlockingStore.SIZE_UNAVAILABLE_FUTURE;
         }

         if (log.isTraceEnabled()) {
            log.tracef("Obtaining approximate size from store %s", firstStoreStatus.store);
         }
         CompletionStage<Long> stage;
         if (firstStoreStatus.hasCharacteristic(Characteristic.SEGMENTABLE)) {
            stage = firstStoreStatus.store.approximateSize(segments);
         } else {
            stage = firstStoreStatus.store.approximateSize(IntSets.immutableRangeSet(segmentCount))
                  .thenApply(size -> {
                     // Counting only the keys in the given segments would be expensive,
                     // so we compute an estimate assuming that each segment has a similar number of entries
                     LocalizedCacheTopology cacheTopology = distributionManager.getCacheTopology();
                     int storeSegments = firstStoreStatus.hasCharacteristic(Characteristic.SHAREABLE) ?
                           segmentCount : cacheTopology.getLocalWriteSegmentsCount();
                     return storeSegments > 0 ? size * segments.size() / storeSegments : size;
                  });
         }
         return stage.whenComplete((ignore, ignoreT) -> releaseReadLock(stamp));
      } catch (Throwable t) {
         releaseReadLock(stamp);
         throw t;
      }
   }

   @Override
   public CompletionStage<Long> size(Predicate<? super StoreConfiguration> predicate, IntSet segments) {
      long stamp = acquireReadLock();
      try {
         checkStoreAvailability();
         if (log.isTraceEnabled()) {
            log.tracef("Obtaining size from stores");
         }
         NonBlockingStore<?, ?> nonBlockingStore = getStoreLocked(storeStatus -> storeStatus.hasCharacteristic(
               Characteristic.BULK_READ) && predicate.test(storeStatus.config));
         if (nonBlockingStore == null) {
            releaseReadLock(stamp);
            return NonBlockingStore.SIZE_UNAVAILABLE_FUTURE;
         }
         if (segments == null) {
            segments = IntSets.immutableRangeSet(segmentCount);
         }
         return nonBlockingStore.size(segments)
               .whenComplete((ignore, ignoreT) -> releaseReadLock(stamp));
      } catch (Throwable t) {
         releaseReadLock(stamp);
         throw t;
      }
   }

   @Override
   public CompletionStage<Long> size(Predicate<? super StoreConfiguration> predicate) {
      return size(predicate, null);
   }

   @Override
   public void setClearOnStop(boolean clearOnStop) {
      this.clearOnStop = clearOnStop;
   }

   @Override
   public CompletionStage<Void> writeToAllNonTxStores(MarshallableEntry marshalledEntry, int segment,
         Predicate<? super StoreConfiguration> predicate, long flags) {
      long stamp = acquireReadLock();
      boolean release = true;
      try {
         if (!checkStoreAvailability()) {
            return CompletableFutures.completedNull();
         }
         if (log.isTraceEnabled()) {
            log.tracef("Writing entry %s for with segment: %d", marshalledEntry, segment);
         }
         // Let the write work in parallel across the stores
         AggregateCompletionStage<Void> stageBuilder = CompletionStages.aggregateCompletionStage();
         for (StoreStatus storeStatus : stores) {
            if (shouldWrite(storeStatus, predicate, flags)) {
               stageBuilder.dependsOn(storeStatus.store.write(segment, marshalledEntry));
            }
         }
         CompletionStage<Void> stage = stageBuilder.freeze();
         if (CompletionStages.isCompletedSuccessfully(stage)) {
            return stage;
         } else {
            release = false;
            return stage.whenComplete((e, throwable) -> releaseReadLock(stamp));
         }
      } finally {
         if (release) {
            releaseReadLock(stamp);
         }
      }
   }

   private int segmentOrZero(StoreStatus storeStatus, int segment) {
      return storeStatus.hasCharacteristic(Characteristic.SEGMENTABLE) ? segment : 0;
   }

   private boolean shouldWrite(StoreStatus storeStatus, Predicate<? super StoreConfiguration> userPredicate) {
      return !storeStatus.hasCharacteristic(Characteristic.READ_ONLY)
            && userPredicate.test(storeStatus.config);
   }

   private boolean shouldWrite(StoreStatus storeStatus, Predicate<? super StoreConfiguration> userPredicate, long flags) {
      return shouldWrite(storeStatus, userPredicate)
            && !storeStatus.store.ignoreCommandWithFlags(flags);
   }

   @Override
   public CompletionStage<Void> prepareAllTxStores(TxInvocationContext<AbstractCacheTransaction> txInvocationContext,
         Predicate<? super StoreConfiguration> predicate) throws PersistenceException {
      Flowable<MVCCEntry<Object, Object>> mvccEntryFlowable = toMvccEntryFlowable(txInvocationContext, null);
      return batchOperation(mvccEntryFlowable, txInvocationContext, (stores, segmentCount, removeFlowable,
            putFlowable) -> stores.prepareWithModifications(txInvocationContext.getTransaction(), segmentCount, removeFlowable, putFlowable))
            .thenApply(CompletableFutures.toNullFunction());
   }

   @Override
   public CompletionStage<Void> commitAllTxStores(TxInvocationContext<AbstractCacheTransaction> txInvocationContext,
         Predicate<? super StoreConfiguration> predicate) {
      long stamp = acquireReadLock();
      boolean release = true;
      try {
         if (!checkStoreAvailability()) {
            return CompletableFutures.completedNull();
         }
         if (log.isTraceEnabled()) {
            log.tracef("Committing transaction %s to stores", txInvocationContext);
         }
         // Let the commit work in parallel across the stores
         AggregateCompletionStage<Void> stageBuilder = CompletionStages.aggregateCompletionStage();
         for (StoreStatus storeStatus : stores) {
            if (shouldPerformTransactionOperation(storeStatus, predicate)) {
               stageBuilder.dependsOn(storeStatus.store.commit(txInvocationContext.getTransaction()));
            }
         }
         CompletionStage<Void> stage = stageBuilder.freeze();
         if (CompletionStages.isCompletedSuccessfully(stage)) {
            return stage;
         } else {
            release = false;
            return stage.whenComplete((e, throwable) -> releaseReadLock(stamp));
         }
      } finally {
         if (release) {
            releaseReadLock(stamp);
         }
      }
   }

   @Override
   public CompletionStage<Void> rollbackAllTxStores(TxInvocationContext<AbstractCacheTransaction> txInvocationContext,
         Predicate<? super StoreConfiguration> predicate) {
      long stamp = acquireReadLock();
      boolean release = true;
      try {
         if (!checkStoreAvailability()) {
            return CompletableFutures.completedNull();
         }
         if (log.isTraceEnabled()) {
            log.tracef("Rolling back transaction %s for stores", txInvocationContext);
         }
         // Let the rollback work in parallel across the stores
         AggregateCompletionStage<Void> stageBuilder = CompletionStages.aggregateCompletionStage();
         for (StoreStatus storeStatus : stores) {
            if (shouldPerformTransactionOperation(storeStatus, predicate)) {
               stageBuilder.dependsOn(storeStatus.store.rollback(txInvocationContext.getTransaction()));
            }
         }
         CompletionStage<Void> stage = stageBuilder.freeze();
         if (CompletionStages.isCompletedSuccessfully(stage)) {
            return stage;
         } else {
            release = false;
            return stage.whenComplete((e, throwable) -> releaseReadLock(stamp));
         }
      } finally {
         if (release) {
            releaseReadLock(stamp);
         }
      }
   }

   private boolean shouldPerformTransactionOperation(StoreStatus storeStatus, Predicate<? super StoreConfiguration> predicate) {
      return storeStatus.hasCharacteristic(Characteristic.TRANSACTIONAL)
            && predicate.test(storeStatus.config);
   }

   @Override
   public <K, V> CompletionStage<Void> writeEntries(Iterable<MarshallableEntry<K, V>> iterable,
         Predicate<? super StoreConfiguration> predicate) {
      return Completable.using(
            this::acquireReadLock,
            ignore -> {
               if (!checkStoreAvailability()) {
                  return Completable.complete();
               }
               if (log.isTraceEnabled()) {
                  log.trace("Writing entries to stores");
               }
               return Flowable.fromIterable(stores)
                     .filter(storeStatus -> shouldWrite(storeStatus, predicate) &&
                           !storeStatus.hasCharacteristic(Characteristic.TRANSACTIONAL))
                     // Let the write work in parallel across the stores
                     .flatMapCompletable(storeStatus -> {
                        boolean segmented = storeStatus.hasCharacteristic(Characteristic.SEGMENTABLE);
                        Flowable<NonBlockingStore.SegmentedPublisher<MarshallableEntry<K, V>>> flowable;
                        if (segmented) {
                           flowable = Flowable.fromIterable(iterable)
                                 .groupBy(groupingFunction(MarshallableEntry::getKey))
                                 .map(SegmentPublisherWrapper::wrap);
                        } else {
                           flowable = Flowable.just(SingleSegmentPublisher.singleSegment(Flowable.fromIterable(iterable)));
                        }
                        return Completable.fromCompletionStage(storeStatus.<K, V>store().batch(segmentCount(segmented),
                              Flowable.empty(), flowable));
                     });
            },
            this::releaseReadLock
      ).toCompletionStage(null);
   }

   @Override
   public CompletionStage<Long> writeMapCommand(PutMapCommand putMapCommand, InvocationContext ctx,
         BiPredicate<? super PutMapCommand, Object> commandKeyPredicate) {
      Flowable<MVCCEntry<Object, Object>> mvccEntryFlowable = entriesFromCommand(putMapCommand, ctx, commandKeyPredicate);
      return batchOperation(mvccEntryFlowable, ctx, NonBlockingStore::batch);
   }

   @Override
   public CompletionStage<Long> performBatch(TxInvocationContext<AbstractCacheTransaction> ctx,
         BiPredicate<? super WriteCommand, Object> commandKeyPredicate) {
      Flowable<MVCCEntry<Object, Object>> mvccEntryFlowable = toMvccEntryFlowable(ctx, commandKeyPredicate);
      return batchOperation(mvccEntryFlowable, ctx, NonBlockingStore::batch);
   }

   /**
    * Takes all the modified entries in the flowable and writes or removes them from the stores in a single batch
    * operation per store.
    * <p>
    * The {@link HandleFlowables} is provided for the sole reason of allowing reuse of this method by different callers.
    * @param mvccEntryFlowable flowable containing modified entries
    * @param ctx the context with modifications
    * @param flowableHandler callback handler that actually should subscribe to the underlying store
    * @param <K> key type
    * @param <V> value type
    * @return a stage that when complete will contain how many write operations were done
    */
   private <K, V> CompletionStage<Long> batchOperation(Flowable<MVCCEntry<K, V>> mvccEntryFlowable, InvocationContext ctx,
         HandleFlowables<K, V> flowableHandler) {
      return Single.using(
            this::acquireReadLock,
            ignore -> {
               if (!checkStoreAvailability()) {
                  return Single.just(0L);
               }
               if (log.isTraceEnabled()) {
                  log.trace("Writing batch to stores");
               }

               return Flowable.fromIterable(stores)
                     .filter(storeStatus -> !storeStatus.hasCharacteristic(Characteristic.READ_ONLY))
                     .flatMapSingle(storeStatus -> {
                        Flowable<MVCCEntry<K, V>> flowableToUse;
                        boolean shared = storeStatus.config.shared();
                        if (shared) {
                           if (log.isTraceEnabled()) {
                              log.tracef("Store %s is shared, checking skip shared stores and ignoring entries not" +
                                    " primarily owned by this node", storeStatus.store);
                           }
                           flowableToUse = mvccEntryFlowable.filter(mvccEntry -> !mvccEntry.isSkipSharedStore());
                        } else {
                           flowableToUse = mvccEntryFlowable;
                        }

                        boolean segmented = storeStatus.config.segmented();

                        // Now we have to split this stores' flowable into two (one for remove and one for put)
                        flowableToUse = flowableToUse.publish().autoConnect(2);

                        Flowable<NonBlockingStore.SegmentedPublisher<Object>> removeFlowable = createRemoveFlowable(
                              flowableToUse, shared, segmented, storeStatus);

                        ByRef.Long writeCount = new ByRef.Long(0);

                        Flowable<NonBlockingStore.SegmentedPublisher<MarshallableEntry<K, V>>> writeFlowable =
                              createWriteFlowable(flowableToUse, ctx, shared, segmented, writeCount, storeStatus);

                        CompletionStage<Void> storeBatchStage = flowableHandler.handleFlowables(storeStatus.store(),
                              segmentCount(segmented), removeFlowable, writeFlowable);

                        return Single.fromCompletionStage(storeBatchStage
                              .thenApply(ignore2 -> writeCount.get()));
                        // Only take the last element for the count - ensures all stores are completed
                     }).last(0L);

            },
            this::releaseReadLock
      ).toCompletionStage();
   }

   private <K, V> Flowable<NonBlockingStore.SegmentedPublisher<Object>> createRemoveFlowable(
         Flowable<MVCCEntry<K, V>> flowableToUse, boolean shared, boolean segmented, StoreStatus storeStatus) {

      Flowable<K> keyRemoveFlowable = flowableToUse
            .filter(MVCCEntry::isRemoved)
            .map(MVCCEntry::getKey);

      Flowable<NonBlockingStore.SegmentedPublisher<Object>> flowable;
      if (segmented) {
         flowable = keyRemoveFlowable
               .groupBy(keyPartitioner::getSegment)
               .map(SegmentPublisherWrapper::wrap);
         flowable = filterSharedSegments(flowable, null, shared);
      } else {
         if (shared && !isInvalidationCache) {
            keyRemoveFlowable = keyRemoveFlowable.filter(k ->
                  distributionManager.getCacheTopology().getDistribution(k).isPrimary());
         }
         flowable = Flowable.just(SingleSegmentPublisher.singleSegment(keyRemoveFlowable));
      }

      if (log.isTraceEnabled()) {
         flowable = flowable.doOnSubscribe(sub ->
               log.tracef("Store %s has subscribed to remove batch", storeStatus.store));
         flowable = flowable.map(sp -> {
            int segment = sp.getSegment();
            return SingleSegmentPublisher.singleSegment(segment, Flowable.fromPublisher(sp)
                  .doOnNext(keyToRemove -> log.tracef("Emitting key %s for removal from segment %s",
                        keyToRemove, segment)));
         });
      }

      return flowable;
   }

   private <K, V> Flowable<NonBlockingStore.SegmentedPublisher<MarshallableEntry<K, V>>> createWriteFlowable(
         Flowable<MVCCEntry<K, V>> flowableToUse, InvocationContext ctx, boolean shared, boolean segmented,
         ByRef.Long writeCount, StoreStatus storeStatus) {

      Flowable<MarshallableEntry<K, V>> entryWriteFlowable = flowableToUse
            .filter(mvccEntry -> !mvccEntry.isRemoved())
            .map(mvcEntry -> {
               K key = mvcEntry.getKey();
               InternalCacheValue<V> sv = internalEntryFactory.getValueFromCtx(key, ctx);
               //noinspection unchecked
               return (MarshallableEntry<K, V>) marshallableEntryFactory.create(key, (InternalCacheValue) sv);
            });

      Flowable<NonBlockingStore.SegmentedPublisher<MarshallableEntry<K, V>>> flowable;
      if (segmented) {
         // Note the writeCount includes entries that aren't written due to being shared
         // at this point
         entryWriteFlowable = entryWriteFlowable.doOnNext(obj -> writeCount.inc());
         flowable = entryWriteFlowable
               .groupBy(me -> keyPartitioner.getSegment(me.getKey()))
               .map(SegmentPublisherWrapper::wrap);
         // The writeCount will be decremented for each grouping of values ignored
         flowable = filterSharedSegments(flowable, writeCount, shared);
      } else {
         if (shared && !isInvalidationCache) {
            entryWriteFlowable = entryWriteFlowable.filter(me ->
                  distributionManager.getCacheTopology().getDistribution(me.getKey()).isPrimary());
         }
         entryWriteFlowable = entryWriteFlowable.doOnNext(obj -> writeCount.inc());
         flowable = Flowable.just(SingleSegmentPublisher.singleSegment(entryWriteFlowable));
      }

      if (log.isTraceEnabled()) {
         flowable = flowable.doOnSubscribe(sub ->
               log.tracef("Store %s has subscribed to write batch", storeStatus.store));
         flowable = flowable.map(sp -> {
            int segment = sp.getSegment();
            return SingleSegmentPublisher.singleSegment(segment, Flowable.fromPublisher(sp)
                  .doOnNext(me -> log.tracef("Emitting entry %s for write to segment %s",
                        me.getKey(), segment)));
         });
      }
      return flowable;
   }

   private <I> Flowable<NonBlockingStore.SegmentedPublisher<I>> filterSharedSegments(
         Flowable<NonBlockingStore.SegmentedPublisher<I>> flowable, ByRef.Long writeCount, boolean shared) {
      if (!shared || isInvalidationCache) {
         return flowable;
      }
      return flowable.map(sp -> {
         if (distributionManager.getCacheTopology().getSegmentDistribution(sp.getSegment()).isPrimary()) {
            return sp;
         }
         Flowable<I> emptyFlowable = Flowable.fromPublisher(sp);
         if (writeCount != null) {
            emptyFlowable = emptyFlowable.doOnNext(ignore -> writeCount.dec())
                  .ignoreElements()
                  .toFlowable();
         } else {
            emptyFlowable = emptyFlowable.take(0);
         }
         // Unfortunately we need to still need to subscribe to the publisher even though we don't want
         // the store to use its values. Thus we just return them an empty SegmentPublisher.
         return SingleSegmentPublisher.singleSegment(sp.getSegment(), emptyFlowable);
      });
   }

   /**
    * Creates a Flowable of MVCCEntry(s) that were modified due to the commands in the transactional context
    * @param ctx the transactional context
    * @param commandKeyPredicate predicate to test if a key/command combination should be written
    * @param <K> key type
    * @param <V> value type
    * @return a Flowable containing MVCCEntry(s) for the modifications in the tx context
    */
   private <K, V> Flowable<MVCCEntry<K, V>> toMvccEntryFlowable(TxInvocationContext<AbstractCacheTransaction> ctx,
         BiPredicate<? super WriteCommand, Object> commandKeyPredicate) {
      return Flowable.fromIterable(ctx.getCacheTransaction().getAllModifications())
            .filter(writeCommand -> !writeCommand.hasAnyFlag(FlagBitSets.SKIP_CACHE_STORE | FlagBitSets.ROLLING_UPGRADE))
            .concatMap(writeCommand -> entriesFromCommand(writeCommand, ctx, commandKeyPredicate));
   }

   private <K, V, WCT extends WriteCommand> Flowable<MVCCEntry<K, V>> entriesFromCommand(WCT writeCommand, InvocationContext ctx,
         BiPredicate<? super WCT, Object> commandKeyPredicate) {
      if (writeCommand instanceof DataWriteCommand) {
         Object key = ((DataWriteCommand) writeCommand).getKey();
         MVCCEntry<K, V> entry = acquireKeyFromContext(ctx, writeCommand, key, commandKeyPredicate);
         return entry != null ? Flowable.just(entry) : Flowable.empty();
      } else {
         if (writeCommand instanceof InvalidateCommand) {
            return Flowable.empty();
         }
         // Assume multiple key command
         return Flowable.fromIterable(writeCommand.getAffectedKeys())
               .concatMapMaybe(key -> {
                  MVCCEntry<K, V> entry = acquireKeyFromContext(ctx, writeCommand, key, commandKeyPredicate);
                  // We use an empty Flowable to symbolize a miss - which is filtered by ofType just below
                  return entry != null ? Maybe.just(entry) : Maybe.empty();
               });
      }
   }

   private <K, V, WCT extends WriteCommand> MVCCEntry<K, V> acquireKeyFromContext(InvocationContext ctx, WCT command, Object key,
         BiPredicate<? super WCT, Object> commandKeyPredicate) {
      if (commandKeyPredicate == null || commandKeyPredicate.test(command, key)) {
         //noinspection unchecked
         MVCCEntry<K, V> entry = (MVCCEntry<K, V>) ctx.lookupEntry(key);
         if (entry.isChanged()) {
            return entry;
         }
      }
      return null;
   }

   /**
    * Here just to create a lambda for method reuse of
    * {@link #batchOperation(Flowable, InvocationContext, HandleFlowables)}
    */
   interface HandleFlowables<K, V> {
      CompletionStage<Void> handleFlowables(NonBlockingStore<K, V> store, int publisherCount,
            Flowable<NonBlockingStore.SegmentedPublisher<Object>> removeFlowable,
            Flowable<NonBlockingStore.SegmentedPublisher<MarshallableEntry<K, V>>> putFlowable);
   }

   /**
    * Provides a function that groups entries by their segments (via keyPartitioner).
    */
   private <E> Function<E, Integer> groupingFunction(Function<E, Object> toKeyFunction) {
      return value -> keyPartitioner.getSegment(toKeyFunction.apply(value));
   }

   /**
    * Returns how many segments the user must worry about when segmented or not.
    * @param segmented whether the store is segmented
    * @return how many segments the store must worry about
    */
   private int segmentCount(boolean segmented) {
      return segmented ? segmentCount : 1;
   }

   @Override
   public boolean isAvailable() {
      return unavailableExceptionMessage == null;
   }

   @Override
   public CompletionStage<Boolean> addSegments(IntSet segments) {
      long stamp = acquireReadLock();
      boolean release = true;
      try {
         if (!checkStoreAvailability()) {
            return CompletableFutures.completedFalse();
         }
         if (log.isTraceEnabled()) {
            log.tracef("Adding segments %s to stores", segments);
         }
         // Let the add work in parallel across the stores
         AggregateCompletionStage<Boolean> stageBuilder = CompletionStages.aggregateCompletionStage(allSegmentedOrShared);
         for (StoreStatus storeStatus : stores) {
            if (shouldInvokeSegmentMethods(storeStatus)) {
               stageBuilder.dependsOn(storeStatus.store.addSegments(segments));
            }
         }
         CompletionStage<Boolean> stage = stageBuilder.freeze();
         if (CompletionStages.isCompletedSuccessfully(stage)) {
            return stage;
         } else {
            release = false;
            return stage.whenComplete((e, throwable) -> releaseReadLock(stamp));
         }
      } finally {
         if (release) {
            releaseReadLock(stamp);
         }
      }
   }

   @Override
   public CompletionStage<Boolean> removeSegments(IntSet segments) {
      long stamp = acquireReadLock();
      boolean release = true;
      try {
         if (!checkStoreAvailability()) {
            return CompletableFutures.completedFalse();
         }
         if (log.isTraceEnabled()) {
            log.tracef("Removing segments %s from stores", segments);
         }
         // Let the add work in parallel across the stores
         AggregateCompletionStage<Boolean> stageBuilder = CompletionStages.aggregateCompletionStage(allSegmentedOrShared);
         for (StoreStatus storeStatus : stores) {
            if (shouldInvokeSegmentMethods(storeStatus)) {
               stageBuilder.dependsOn(storeStatus.store.removeSegments(segments));
            }
         }
         CompletionStage<Boolean> stage = stageBuilder.freeze();
         if (CompletionStages.isCompletedSuccessfully(stage)) {
            return stage;
         } else {
            release = false;
            return stage.whenComplete((e, throwable) -> releaseReadLock(stamp));
         }
      } finally {
         if (release) {
            releaseReadLock(stamp);
         }
      }
   }

   private static boolean shouldInvokeSegmentMethods(StoreStatus storeStatus) {
      return storeStatus.hasCharacteristic(Characteristic.SEGMENTABLE) &&
            !storeStatus.hasCharacteristic(Characteristic.SHAREABLE);
   }

   public <K, V> List<NonBlockingStore<K, V>> getAllStores(Predicate<Set<Characteristic>> predicate) {
      long stamp = acquireReadLock();
      try {
         if (!checkStoreAvailability()) {
            return Collections.emptyList();
         }
         return stores.stream()
               .filter(storeStatus -> predicate.test(storeStatus.characteristics))
               .map(StoreStatus::<K, V>store)
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
    * Method must be here for augmentation to tell blockhound this method is okay to block
    */
   private long acquireWriteLock() {
      return lock.writeLock();
   }

   /**
    * Opposite of acquireReadLock here for symmetry
    */
   private void releaseReadLock(long stamp) {
      lock.unlockRead(stamp);
   }

   /**
    * Opposite of acquireWriteLock here for symmetry
    */
   private void releaseWriteLock(long stamp) {
      lock.unlockWrite(stamp);
   }

   private boolean checkStoreAvailability() {
      if (!enabled) return false;

      String message = unavailableExceptionMessage;
      if (message != null) {
         throw new StoreUnavailableException(message);
      }

      // Stores will be null if this is not started or was stopped and not restarted.
      if (stores == null) {
         throw new IllegalLifecycleStateException();
      }
      return true;
   }

   static class StoreStatus {
      final NonBlockingStore<?, ?> store;
      final StoreConfiguration config;
      final Set<Characteristic> characteristics;
      // This variable is protected by PersistenceManagerImpl#lock and also the fact that availability check can
      // only be ran one at a time
      boolean availability = true;

      StoreStatus(NonBlockingStore<?, ?> store, StoreConfiguration config, Set<Characteristic> characteristics) {
         this.store = store;
         this.config = config;
         this.characteristics = characteristics;
      }

      <K, V> NonBlockingStore<K, V> store() {
         return (NonBlockingStore) store;
      }

      private boolean hasCharacteristic(Characteristic characteristic) {
         return characteristics.contains(characteristic);
      }
   }

   boolean anyLocksHeld() {
      return lock.isReadLocked() || lock.isWriteLocked();
   }
}
