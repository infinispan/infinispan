package org.infinispan.persistence.manager;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.util.logging.Log.PERSISTENCE;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiPredicate;
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
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.WriteCommand;
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
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
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
import org.infinispan.persistence.internal.PersistenceUtil;
import org.infinispan.persistence.spi.LocalOnlyCacheLoader;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.NonBlockingStore.Characteristic;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.spi.StoreUnavailableException;
import org.infinispan.persistence.support.ComposedSegmentedLoadWriteStore;
import org.infinispan.persistence.support.DelegatingNonBlockingStore;
import org.infinispan.persistence.support.NonBlockingStoreAdapter;
import org.infinispan.persistence.support.SegmentPublisherWrapper;
import org.infinispan.persistence.support.SingleSegmentPublisher;
import org.infinispan.transaction.impl.AbstractCacheTransaction;
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
import io.reactivex.rxjava3.flowables.ConnectableFlowable;
import io.reactivex.rxjava3.functions.Function;
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
   @Inject InternalEntryFactory internalEntryFactory;
   @Inject MarshallableEntryFactory<?, ?> marshallableEntryFactory;
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
   private final List<StoreStatus> stores = new ArrayList<>(4);

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
      for (StoreStatus storeStatus : stores) {
         if (predicate.test(storeStatus)) {
            return storeStatus.store();
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
                  CompletionStage<Void> stage = nonBlockingStore.start(ctx).whenComplete((ignore, t) -> {
                     // On exception, just put a status with only the store - this way we can still invoke stop on it later
                     if (t != null) {
                        stores.add(new StoreStatus(nonBlockingStore, null, null));
                     }
                  });
                  return Completable.fromCompletionStage(stage)
                        .toSingle(() -> new StoreStatus(nonBlockingStore, processedConfiguration,
                              updateCharacteristics(nonBlockingStore, nonBlockingStore.characteristics(), storeConfiguration)));
               })
               // This relies upon visibility guarantees of reactive streams for publishing map values
               .doOnNext(stores::add)
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
         log.debug("PersistenceManagerImpl encountered an exception during startup of stores", t);
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
      if (!storeConfiguration.segmented()) {
         characteristics.remove(Characteristic.SEGMENTABLE);
      }
      return characteristics;
   }

   /**
    * Polls the availability of all configured stores. If a store is found to be unavailable all future requests
    * to this manager will encounter an StoreUnavailableException. Note that this method should only have one availability
    * check running at a time. This is currently guaranteed as it is using a non overlapping scheduler.
    * @return stage that signals that all store availability checks are done
    */
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
            ignore -> Flowable.fromIterable(stores)
                  .flatMapMaybe(storeStatus -> {
                     CompletionStage<Boolean> availableStage = storeStatus.store.isAvailable();
                     return Maybe.fromCompletionStage(availableStage.thenApply(isAvailable -> {
                        storeStatus.availability = isAvailable;
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
         Flowable<NonBlockingStore<?, ?>> flowable = Flowable.fromIterable(stores)
               .map(StoreStatus::store);
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
      NonBlockingStore<Object, Object> nonBlockingStore = getStoreLocked(status -> status.config.preload());
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
            .concatMapSingle(me -> preloadEntry(flags, me, keyDataConversion, valueDataConversion))
            .count()
            .toCompletionStage()
            .thenAccept(insertAmount -> {
               this.preloaded = insertAmount < maxEntries;
               log.debugf("Preloaded %d keys in %s", insertAmount, Util.prettyPrintTime(timeService.timeDuration(start, MILLISECONDS)));
            });
   }

   private Single<Object> preloadEntry(long flags, MarshallableEntry<Object, Object> me, DataConversion keyDataConversion, DataConversion valueDataConversion) {
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
         //noinspection unchecked
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

   @GuardedBy("lock#readLock")
   private long getFlagsForStateInsertion() {
      long flags = FlagBitSets.CACHE_MODE_LOCAL |
            FlagBitSets.SKIP_OWNERSHIP_CHECK |
            FlagBitSets.IGNORE_RETURN_VALUES |
            FlagBitSets.SKIP_CACHE_STORE |
            FlagBitSets.SKIP_LOCKING |
            FlagBitSets.SKIP_XSITE_BACKUP |
            FlagBitSets.IRAC_STATE;

      boolean hasSharedStore = getStoreLocked(storeStatus -> storeStatus.config.shared()) != null;

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
         return stores.stream()
               .map(storeStatus -> storeStatus.store.getClass().getName())
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
         for (StoreStatus storeStatus : stores) {
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
               return Flowable.fromIterable(stores)
                     .filter(storeStatus ->
                           !storeStatus.characteristics.contains(Characteristic.READ_ONLY)
                                 && predicate.test(storeStatus.config))
                     // Let the clear work in parallel across the stores
                     .flatMapCompletable(storeStatus -> Completable.fromCompletionStage(
                           storeStatus.store.clear()));
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
               return Flowable.fromIterable(stores)
                     .filter(storeStatus ->
                           !storeStatus.characteristics.contains(Characteristic.READ_ONLY)
                                 && predicate.test(storeStatus.config))
                     // Let the delete work in parallel across the stores
                     .flatMapSingle(storeStatus -> Single.fromCompletionStage(
                           storeStatus.store.delete(segment, key)))
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
   public <K, V> Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter,
         boolean fetchValue, boolean fetchMetadata, Predicate<? super StoreConfiguration> predicate) {
      return Flowable.using(this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.tracef("Publishing entries for segments %s", segments);
               }
               for (StoreStatus storeStatus : stores) {
                  Set<Characteristic> characteristics = storeStatus.characteristics;
                  if (characteristics.contains(Characteristic.BULK_READ) &&  predicate.test(storeStatus.config)) {
                     Predicate<? super K> filterToUse;
                     if (!characteristics.contains(Characteristic.SEGMENTABLE)) {
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
               checkStoreAvailability();
               if (trace) {
                  log.tracef("Publishing keys for segments %s", segments);
               }
               for (StoreStatus storeStatus : stores) {
                  Set<Characteristic> characteristics = storeStatus.characteristics;
                  if (characteristics.contains(Characteristic.BULK_READ) &&  predicate.test(storeStatus.config)) {
                     Predicate<? super K> filterToUse;
                     if (!characteristics.contains(Characteristic.SEGMENTABLE)) {
                        filterToUse = PersistenceUtil.combinePredicate(segments, keyPartitioner, filter);
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
      return Maybe.using(
            this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.tracef("Loading entry for key %s with segment %d", key, segment);
               }
               return Flowable.fromIterable(stores)
                     .filter(storeStatus -> allowLoad(storeStatus, localInvocation, includeStores))
                     // Only do 1 request at a time
                     .concatMapMaybe(storeStatus -> Maybe.fromCompletionStage(
                           storeStatus.<K, V>store().load(segmentOrZero(storeStatus, segment), key)), 1)
                     .firstElement();
            },
            this::releaseReadLock
      ).toCompletionStage(null);
   }

   private boolean allowLoad(StoreStatus storeStatus, boolean localInvocation, boolean includeStores) {
      return !storeStatus.characteristics.contains(Characteristic.WRITE_ONLY) && (localInvocation || !isLocalOnlyLoader(storeStatus.store)) &&
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
               return Flowable.fromIterable(stores)
                     .filter(storeStatus -> shouldWrite(storeStatus, predicate, flags))
                     // Let the write work in parallel across the stores
                     .flatMapCompletable(storeStatus -> Completable.fromCompletionStage(storeStatus.store.write(segmentOrZero(storeStatus, segment), marshalledEntry)));
            },
            this::releaseReadLock
      ).toCompletionStage(null);
   }

   private int segmentOrZero(StoreStatus storeStatus, int segment) {
      return storeStatus.characteristics.contains(Characteristic.SEGMENTABLE) ? segment : 0;
   }

   private boolean shouldWrite(StoreStatus storeStatus, Predicate<? super StoreConfiguration> userPredicate) {
      return !storeStatus.characteristics.contains(Characteristic.READ_ONLY)
            && userPredicate.test(storeStatus.config);
   }

   private boolean shouldWrite(StoreStatus storeStatus, Predicate<? super StoreConfiguration> userPredicate, long flags) {
      return shouldWrite(storeStatus, userPredicate)
            && !storeStatus.store.ignoreCommandWithFlags(flags);
   }

   @Override
   public CompletionStage<Void> prepareAllTxStores(TxInvocationContext<AbstractCacheTransaction> txInvocationContext,
         Predicate<? super StoreConfiguration> predicate) throws PersistenceException {
      //noinspection unchecked
      return batchOperation(txInvocationContext, null, (stores, segmentCount, removeFlowable,
            putFlowable) -> stores.prepareWithModifications(txInvocationContext.getTransaction(), segmentCount, removeFlowable, putFlowable))
            .thenApply(CompletableFutures.toNullFunction());
   }

   @Override
   public CompletionStage<Void> commitAllTxStores(TxInvocationContext<AbstractCacheTransaction> txInvocationContext,
         Predicate<? super StoreConfiguration> predicate) {
      return Completable.using(
            this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.tracef("Committing transaction %s to stores", txInvocationContext);
               }
               return Flowable.fromIterable(stores)
                     .filter(storeStatus -> shouldPerformTransactionOperation(storeStatus, predicate))
                     // Let the commit work in parallel across the stores
                     .flatMapCompletable(storeStatus -> Completable.fromCompletionStage(storeStatus.store.commit(txInvocationContext.getTransaction())));
            },
            this::releaseReadLock
      ).toCompletionStage(null);
   }

   @Override
   public CompletionStage<Void> rollbackAllTxStores(TxInvocationContext<AbstractCacheTransaction> txInvocationContext,
         Predicate<? super StoreConfiguration> predicate) {
      return Completable.using(
            this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.tracef("Rolling back transaction %s for stores", txInvocationContext);
               }
               return Flowable.fromIterable(stores)
                     .filter(storeStatus -> shouldPerformTransactionOperation(storeStatus, predicate))
                     // Let the rollback work in parallel across the stores
                     .flatMapCompletable(storeStatus -> Completable.fromCompletionStage(storeStatus.store.rollback(txInvocationContext.getTransaction())));
            },
            this::releaseReadLock
      ).toCompletionStage(null);
   }

   private boolean shouldPerformTransactionOperation(StoreStatus storeStatus, Predicate<? super StoreConfiguration> predicate) {
      return storeStatus.characteristics.contains(Characteristic.TRANSACTIONAL)
            && predicate.test(storeStatus.config);
   }

   @Override
   public <K, V> CompletionStage<Void> writeBatchToAllNonTxStores(Iterable<MarshallableEntry<K, V>> entries, Predicate<? super StoreConfiguration> predicate, long flags) {
      return Completable.using(
            this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.trace("Writing batch iterable to stores");
               }
               return Flowable.fromIterable(stores)
                     .filter(storeStatus -> shouldWrite(storeStatus, predicate, flags))
                     // Let the rollback work in parallel across the stores
                     .flatMapCompletable(storeStatus -> {
                        boolean segmented = storeStatus.characteristics.contains(Characteristic.SEGMENTABLE);
                        Flowable<NonBlockingStore.SegmentedPublisher<MarshallableEntry<K, V>>> flowable;
                        if (segmented) {
                           flowable = Flowable.fromIterable(entries)
                                 .groupBy(groupingFunction(MarshallableEntry::getKey))
                                 .map(SegmentPublisherWrapper::wrap);
                        } else {
                           flowable = Flowable.just(SingleSegmentPublisher.singleSegment(Flowable.fromIterable(entries)));
                        }

                        return Completable.fromCompletionStage(
                              storeStatus.<K, V>store().batch(segmentCount(segmented), Flowable.empty(), flowable));
                     });
            },
            this::releaseReadLock
      ).toCompletionStage(null);
   }

   @Override
   public CompletionStage<Long> performBatch(TxInvocationContext<AbstractCacheTransaction> ctx,
         BiPredicate<WriteCommand, Object> commandKeyPredicate) {
      return batchOperation(ctx, commandKeyPredicate, NonBlockingStore::batch);
   }

   /**
    * Takes all the modifications from the transaction in the provided context and streams those values as remove or
    * put modifications to the underlying store(s).
    * <p>
    * This method ensures only a single subscription to the underlying modifications that is shared with all of the
    * stores, by using a {@link ConnectableFlowable} to broadcast the values to each. We also then use another
    * {@code ConnectableFlowable} for each store when broadcasting the values for the put and removed entries.
    * Note that due to this if a store doesn't subscribe to either of the put or remove flowable that the operation will
    * not complete.
    * <p>
    * The {@link HandleFlowables} is provided for the sole reason of allowing reuse of this method by different callers.
    * @param ctx the transactional context with modifications
    * @param commandKeyPredicate predicate to test if a key/command combination should be written
    * @param flowableHandler callback handler that actually should subscribe to the underlying store
    * @param <K> key type
    * @param <V> value type
    * @return a stage that when complete will contain how many write operations were done
    */
   private <K, V> CompletionStage<Long> batchOperation(TxInvocationContext<AbstractCacheTransaction> ctx,
         BiPredicate<WriteCommand, Object> commandKeyPredicate, HandleFlowables<K, V> flowableHandler) {
      return Single.using(
            this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.trace("Writing batch to stores");
               }
               int size = stores.size();
               if (size == 0) {
                  return Single.just(0L);
               }

               Flowable<MVCCEntry<K, V>> mvccEntryFlowable = Flowable.fromIterable(ctx.getCacheTransaction().getAllModifications())
                     .filter(writeCommand -> !writeCommand.hasAnyFlag(FlagBitSets.SKIP_CACHE_STORE))
                     .concatMap(writeCommand -> {
                        if (writeCommand instanceof DataWriteCommand) {
                           Object key = ((DataWriteCommand) writeCommand).getKey();
                           MVCCEntry<K, V> entry = acquireKeyFromContext(ctx, writeCommand, key, commandKeyPredicate);
                           return entry != null ? Flowable.just(entry) : Flowable.empty();
                        } else {
                           // Assume multiple key command
                           return Flowable.fromIterable(writeCommand.getAffectedKeys())
                                 .concatMapMaybe(key -> {
                                    MVCCEntry<K, V> entry = acquireKeyFromContext(ctx, writeCommand, key, commandKeyPredicate);
                                    // We use an empty Flowable to symbolize a miss - which is filtered by ofType just below
                                    return entry != null ? Maybe.just(entry) : Maybe.empty();
                                 });
                        }
                     });
               // We split the original source into possibly multiple, depending upon how many stores there are
               ConnectableFlowable<MVCCEntry<K, V>> connectableFlowable = mvccEntryFlowable.publish();

               // We may have multiple stores - don't publish anything until this value goes back to 0
               AtomicInteger pendingConnects = new AtomicInteger(size);
               if (trace) {
                  log.tracef("Will wait for %s subscriptions of stores", size);
               }

               return Flowable.fromIterable(stores)
                     .filter(storeStatus -> {
                        if (storeStatus.characteristics.contains(Characteristic.READ_ONLY)) {
                           int pendingCount = pendingConnects.decrementAndGet();
                           if (trace) {
                              log.tracef("Store %s was read only, decrementing pending count is now %s",
                                    storeStatus.store, pendingCount);
                           }
                           if (pendingCount == 0) {
                              connectableFlowable.connect();
                           }
                           return false;
                        }
                        return true;
                     })
                     .flatMapSingle(storeStatus -> {
                        Flowable<MVCCEntry<K, V>> flowableToUse;
                        if (storeStatus.config.shared()) {
                           flowableToUse = connectableFlowable.filter(mvccEntry -> !mvccEntry.isSkipSharedStore());
                        } else {
                           flowableToUse = connectableFlowable;
                        }
                        flowableToUse = flowableToUse.doOnSubscribe(sub -> {
                           int pendingCount = pendingConnects.decrementAndGet();
                           if (trace) {
                              log.tracef("Store %s has fully subscribed, decrementing pending count is now %s",
                                    storeStatus.store, pendingCount);
                           }
                           if (pendingCount == 0) {
                              connectableFlowable.connect();
                           }
                        });

                        boolean segmented = storeStatus.config.segmented();

                        // Now we have to split this stores' flowable into two (one for remove and one for put)
                        flowableToUse = flowableToUse.publish().autoConnect(2);

                        Flowable<K> keyRemoveFlowable = flowableToUse
                              .filter(MVCCEntry::isRemoved)
                              .map(MVCCEntry::getKey);

                        Flowable<NonBlockingStore.SegmentedPublisher<Object>> removeFlowable;
                        if (segmented) {
                           removeFlowable = keyRemoveFlowable
                                 .groupBy(keyPartitioner::getSegment)
                                 .map(SegmentPublisherWrapper::wrap);
                        } else {
                           removeFlowable = Flowable.just(SingleSegmentPublisher.singleSegment(keyRemoveFlowable));
                        }

                        if (trace) {
                           removeFlowable = removeFlowable.doOnSubscribe(sub ->
                              log.tracef("Store %s has subscribed to remove batch", storeStatus.store));
                        }

                        Flowable<MarshallableEntry<K, V>> entryWriteFlowable = flowableToUse
                              .filter(mvccEntry -> !mvccEntry.isRemoved())
                              .map(mvcEntry -> {
                                 K key = mvcEntry.getKey();
                                 InternalCacheValue<V> sv = internalEntryFactory.getValueFromCtx(key, ctx);
                                 //noinspection unchecked
                                 return (MarshallableEntry<K, V>) marshallableEntryFactory.create(key, (InternalCacheValue) sv);
                              });

                        AtomicLong writeCount = new AtomicLong();
                        entryWriteFlowable = entryWriteFlowable.doOnNext(me -> writeCount.incrementAndGet());

                        Flowable<NonBlockingStore.SegmentedPublisher<MarshallableEntry<K, V>>> writeFlowable;

                        if (segmented) {
                           writeFlowable = entryWriteFlowable
                                 .groupBy(me -> keyPartitioner.getSegment(me.getKey()))
                                 .map(SegmentPublisherWrapper::wrap);
                        } else {
                           writeFlowable = Flowable.just(SingleSegmentPublisher.singleSegment(entryWriteFlowable));
                        }

                        if (trace) {
                           removeFlowable = removeFlowable.doOnSubscribe(sub ->
                              log.tracef("Store %s has subscribed to write batch", storeStatus.store));
                        }

                        return Single.fromCompletionStage(flowableHandler.handleFlowables(storeStatus.store(),
                              segmentCount(segmented), removeFlowable, writeFlowable)
                              .thenApply(ignore2 -> writeCount.get()));
                     }).first(0L);

            },
            this::releaseReadLock
      ).toCompletionStage();
   }

   private <K, V> MVCCEntry<K, V> acquireKeyFromContext(InvocationContext ctx, WriteCommand command, Object key,
         BiPredicate<WriteCommand, Object> commandKeyPredicate) {
      if (commandKeyPredicate.test(command, key)) {
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
    * {@link #batchOperation(TxInvocationContext, BiPredicate, HandleFlowables)}
    */
   interface HandleFlowables<K, V> {
      CompletionStage<Void> handleFlowables(NonBlockingStore<K, V> store, int publisherCount,
            Flowable<NonBlockingStore.SegmentedPublisher<Object>> removeFlowable,
            Flowable<NonBlockingStore.SegmentedPublisher<MarshallableEntry<K, V>>> putFlowable);
   };

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
      return Single.using(
            this::acquireReadLock,
            ignore -> {
               checkStoreAvailability();
               if (trace) {
                  log.tracef("Adding segments %s to stores", segments);
               }
               int size = stores.size();
               return Flowable.fromIterable(stores)
                     .filter(storeStatus -> storeStatus.characteristics.contains(Characteristic.SEGMENTABLE))
                     .delay(storeStatus -> Completable.fromCompletionStage(storeStatus.store.addSegments(segments)).toFlowable())
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
               int size = stores.size();
               return Flowable.fromIterable(stores)
                     .filter(storeStatus -> storeStatus.characteristics.contains(Characteristic.SEGMENTABLE))
                     .delay(storeStatus -> Completable.fromCompletionStage(storeStatus.store.removeSegments(segments)).toFlowable())
                     .count()
                     .map(count -> size == count);
            },
            this::releaseReadLock
      ).toCompletionStage();
   }

   public <K, V> List<NonBlockingStore<K, V>> getAllStores(Predicate<Set<Characteristic>> predicate) {
      long stamp = acquireReadLock();
      try {
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
   }

   boolean anyLocksHeld() {
      return lock.isReadLocked() || lock.isWriteLocked();
   }
}
