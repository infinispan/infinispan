package org.infinispan.persistence.async;

import java.lang.invoke.MethodHandles;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.reactive.RxJavaInterop;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.PersistenceConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.support.DelegatingNonBlockingStore;
import org.infinispan.persistence.support.SegmentPublisherWrapper;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.flowables.ConnectableFlowable;
import io.reactivex.rxjava3.functions.Function;
import net.jcip.annotations.GuardedBy;

/**
 * A delegating NonBlockingStore implementation that batches write operations and runs the resulting batches on the
 * delegate store in a non overlapping manner. That is that only a single batch will be running at a time.
 * <p>
 * Whenever a write operation is performed it will also attempt to start a batch write immediately to the delegate store.
 * Any concurrent writes during this time may be included in the batch. Any additional writes will be enqueued until
 * the batch completes in which case it will automatically submit the pending batch, if there is one.  Write operations
 * to the same key in the same batch will be coalesced with only the last write being written to the underlying store.
 * If the number of enqueued pending write operations becomes equal or larger than the modification queue, then any
 * subsequent write will be added to the queue, but the returned Stage will not complete until the current batch completes
 * in an attempt to provide some backpressure to slow writes.
 * <p>
 * Read operations may be resolved by this store immediately if the given key is still being updated in the
 * delegate store or if it is enqueued for the next batch. If the key is in neither it will query the underlying store
 * to acquire it.
 * @author wburns
 * @since 11.0
 * @param <K> key type for the store
 * @param <V> value type for the store
 */
public class AsyncNonBlockingStore<K, V> extends DelegatingNonBlockingStore<K, V> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private final NonBlockingStore<K, V> actual;

   private Executor nonBlockingExecutor;
   private int segmentCount;
   private int modificationQueueSize;
   private PersistenceConfiguration persistenceConfiguration;
   private AsyncStoreConfiguration asyncConfiguration;

   // "Non blocking" scheduler used for the purpose of delaying retry batch operations on failures
   private ScheduledExecutorService scheduler;

   // This variable will be non null if there is a pending batch being sent to the underlying store
   // If a request causes the modification queue to overflow it will receive a stage back that is only complete
   // when this future is completed (aka. previous replication has completed)
   @GuardedBy("this")
   private CompletableFuture<Void> batchFuture;

   // This variable will be non null if the underlying store has been found to be not available
   // Note that the async store will still be available as long as the queue size (ie. modificationMap.size) is not
   // greater than the configured modificationQueueSize
   @GuardedBy("this")
   private CompletableFuture<Void> delegateAvailableFuture;

   // Any pending modifications will be enqueued in this map
   @GuardedBy("this")
   private Map<Object, Modification> pendingModifications = new HashMap<>();
   // If there is a pending clear this will be true
   @GuardedBy("this")
   private boolean hasPendingClear;
   // The next two variables are held temporarily until a replication of the values is complete. We need to retain
   // these values until we are sure the entries are actually in the store - note these variables are only written to
   // via reference (thus the map is safe to read outside of this lock, but the reference must be read in synchronized)
   // This map contains all the modifications currently being replicated to the delegating store
   @GuardedBy("this")
   private Map<Object, Modification> replicatingModifications = Collections.emptyMap();
   // True if there is an outstanding clear that is being ran on the delegating store
   @GuardedBy("this")
   private boolean isReplicatingClear;
   private volatile boolean stopped = true;

   public AsyncNonBlockingStore(NonBlockingStore<K, V> actual) {
      this.actual = actual;
   }

   @Override
   public Set<Characteristic> characteristics() {
      // Here solely for documentation purposes. This store supports only the characteristics of
      // the underlying store
      return super.characteristics();
   }

   @Override
   public CompletionStage<Void> start(InitializationContext ctx) {
      Configuration cacheConfiguration = ctx.getCache().getCacheConfiguration();
      persistenceConfiguration = cacheConfiguration.persistence();
      scheduler = ctx.getCache().getCacheManager().getGlobalComponentRegistry().getComponent(
            ScheduledExecutorService.class, KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR);
      assert scheduler != null;
      StoreConfiguration storeConfiguration = ctx.getConfiguration();
      segmentCount = storeConfiguration.segmented() ? cacheConfiguration.clustering().hash().numSegments() : 1;
      asyncConfiguration = storeConfiguration.async();
      modificationQueueSize = asyncConfiguration.modificationQueueSize();
      nonBlockingExecutor = ctx.getNonBlockingExecutor();
      stopped = false;
      return actual.start(ctx);
   }

   @Override
   public CompletionStage<Void> stop() {
      CompletionStage<Void> asyncStage;
      if (log.isTraceEnabled()) {
         log.tracef("Stopping async store containing store %s", actual);
      }
      asyncStage = awaitQuiescence();
      return asyncStage.thenCompose(ignore -> {
         if (log.isTraceEnabled()) {
            log.tracef("Stopping store %s from async store", actual);
         }
         stopped = true;
         return actual.stop();
      });
   }

   /**
    * Returns a stage that when complete, this store has submitted and completed all pending modifications
    */
   private CompletionStage<Void> awaitQuiescence() {
      CompletionStage<Void> stage;
      synchronized (this) {
         stage = batchFuture;
      }
      if (stage == null) {
         return CompletableFutures.completedNull();
      }
      if (log.isTraceEnabled()) {
         log.tracef("Must wait until prior batch completes for %s", actual);
      }
      return stage.thenCompose(ignore -> awaitQuiescence());
   }

   void putModification(Object key, Modification modification) {
      pendingModifications.put(key, modification);
   }

   void putClearModification() {
      pendingModifications.clear();
      hasPendingClear = true;
   }

   /**
    * This method submits a batch of modifications to the underlying store and completes {@code batchFuture}
    * when the modifications are done.
    *
    * If there are any pending modifications at that time, it automatically submits a new batch,
    * otherwise it sets {@code batchFuture} to null.
    *
    * Callers must atomically check that {@code batchFuture} is null and set it to a non-null value,
    * to ensure that only one batch is being processed at any time.
    */
   private void submitTask() {
      Map<Object, Modification> newMap = new HashMap<>();
      if (log.isTraceEnabled()) {
         log.tracef("Starting new batch with id %s", System.identityHashCode(newMap));
      }
      boolean ourClearToReplicate;
      Map<Object, Modification> ourModificationsToReplicate;
      synchronized (this) {
         // The isReplicatingClear would be true or replicatingModifications non empty if an update was currently pending
         // But we should only allow one at a time
         assert replicatingModifications.isEmpty() && !isReplicatingClear;
         replicatingModifications = pendingModifications;
         ourModificationsToReplicate = pendingModifications;
         pendingModifications = newMap;
         isReplicatingClear = hasPendingClear;
         ourClearToReplicate = hasPendingClear;
         hasPendingClear = false;
      }

      CompletionStage<Void> asyncBatchStage;
      if (ourClearToReplicate) {
         if (log.isTraceEnabled()) {
            log.tracef("Sending clear to underlying store for id %s", System.identityHashCode(ourModificationsToReplicate));
         }
         asyncBatchStage = retry(actual::clear, persistenceConfiguration.connectionAttempts()).whenComplete((ignore, t) -> {
            synchronized (this) {
               isReplicatingClear = false;
            }
         });
      } else {
         asyncBatchStage = CompletableFutures.completedNull();
      }

      if (!ourModificationsToReplicate.isEmpty()) {
         asyncBatchStage = asyncBatchStage.thenCompose(ignore -> {
            if (log.isTraceEnabled()) {
               log.tracef("Sending batch of %d write/remove operations to underlying store with id %s", ourModificationsToReplicate.size(),
                     System.identityHashCode(ourModificationsToReplicate));
            }
            return retry(() -> replicateModifications(ourModificationsToReplicate), persistenceConfiguration.connectionAttempts())
                  .whenComplete((ignore2, t) -> {
               synchronized (this) {
                  replicatingModifications = Collections.emptyMap();
               }
            });
         });
      }

      asyncBatchStage.whenComplete((ignore, t) -> {
         if (log.isTraceEnabled()) {
            log.tracef("Async operations completed for id %s", System.identityHashCode(ourModificationsToReplicate));
         }
         boolean submitNewBatch;
         CompletableFuture<Void> future;
         synchronized (this) {
            submitNewBatch = !pendingModifications.isEmpty() || hasPendingClear;
            future = batchFuture;
            batchFuture = submitNewBatch ? new CompletableFuture<>() : null;
         }
         if (t != null) {
            future.completeExceptionally(t);
         } else {
            future.complete(null);
         }
         if (submitNewBatch) {
            if (log.isTraceEnabled()) {
               log.trace("Submitting new batch after completion of prior");
            }
            submitTask();
         }
      });
   }

   /**
    * Attempts to run the given supplier, checking the stage if it contains an error. It will rerun the Supplier
    * until a supplied stage doesn't contain an exception or it has encountered retries amount of exceptions. In the
    * latter case it will complete the returned stage with the last throwable encountered.
    * <p>
    * The supplier is only invoked on the delegating store if it is actually available and will wait for it to
    * become so if necessary.
    * @param operationSupplier supplies the stage to test if a throwable was encountered
    * @param retries how many attempts to make before giving up and propagating the exception
    * @return a stage that is completed when the underlying supplied stage completed normally or has encountered a
    * throwable retries times
    */
   private CompletionStage<Void> retry(Supplier<CompletionStage<Void>> operationSupplier, int retries) {
      return CompletionStages.handleAndCompose(getAvailabilityDelayStage().thenCompose(ignore -> operationSupplier.get()), (ignore, throwable) -> {
         if (throwable != null) {
            if (retries > 0) {
               int waitTime = persistenceConfiguration.availabilityInterval();
               log.debugf(throwable,"Failed to process async operation - retrying with delay of %d ms", waitTime);
               if (waitTime > 0) {
                  RunnableCompletionStage rcs = new RunnableCompletionStage(() -> retry(operationSupplier,retries - 1));
                  scheduler.schedule(rcs, waitTime, TimeUnit.MILLISECONDS);
                  return rcs;
               }
               return retry(operationSupplier,retries - 1);
            } else {
               log.debug("Failed to process async operation - no more retries", throwable);
               return CompletableFutures.completedExceptionFuture(throwable);
            }
         }
         return CompletableFutures.completedNull();
      });
   }

   private static class RunnableCompletionStage extends CompletableFuture<Void> implements Runnable {
      private final Supplier<CompletionStage<Void>> supplier;

      private RunnableCompletionStage(Supplier<CompletionStage<Void>> supplier) {
         this.supplier = supplier;
      }

      @Override
      public void run() {
         supplier.get().whenComplete((ignore, throwable) -> {
            if (throwable != null) {
               completeExceptionally(throwable);
            } else {
               complete(null);
            }
         });
      }
   }

   private CompletionStage<Void> replicateModifications(Map<Object, Modification> modifications) {
      // Use a connected flowable, so we don't have to iterate over the modifications twice
      ConnectableFlowable<Modification> connectableModifications = Flowable.fromIterable(modifications.values())
            .publish();

      // The method below may subscribe to the Flowable on a different thread, thus we must auto connect after both are
      // subscribed to (e.g. NonBlockingStoreAdapter subscribes on a blocking thread)
      Flowable<Modification> modificationFlowable = connectableModifications.autoConnect(2);

      return actual.batch(segmentCount,
            modificationFlowable.ofType(RemoveModification.class)
                  .groupBy(Modification::getSegment, RemoveModification::getKey)
                  .map(SegmentPublisherWrapper::wrap),
            modificationFlowable.ofType(PutModification.class)
                  .groupBy(Modification::getSegment, PutModification::<K, V>getEntry)
                  .map(SegmentPublisherWrapper::wrap));
   }

   private CompletionStage<Void> getAvailabilityDelayStage() {
      if (asyncConfiguration.failSilently()) {
         return CompletableFutures.completedNull();
      }
      CompletableFuture<Void> availabilityFuture;
      synchronized (this) {
         availabilityFuture = delegateAvailableFuture;
      }
      return availabilityFuture == null ? CompletableFutures.completedNull() : availabilityFuture;
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean includeValues) {
      return Flowable.defer(() -> {
         assertNotStopped();
         if (log.isTraceEnabled()) {
            log.tracef("Publisher subscribed to retrieve entries for segments %s", segments);
         }
         return abstractPublish(segments, filter, PutModification::getEntry, MarshallableEntry::getKey,
               (innerSegments, predicate) -> actual.publishEntries(innerSegments, predicate, includeValues));
      });
   }

   @Override
   public Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
      return Flowable.defer(() -> {
         assertNotStopped();
         if (log.isTraceEnabled()) {
            log.tracef("Publisher subscribed to retrieve keys for segments %s", segments);
         }
         return abstractPublish(segments, filter, putModification -> putModification.<K, Object>getEntry().getKey(),
               RxJavaInterop.identityFunction(), actual::publishKeys);
      });
   }

   private <E> Publisher<E> abstractPublish(IntSet segments, Predicate<? super K> filter, Function<PutModification, E> putFunction,
         Function<E, K> toKeyFunction, BiFunction<IntSet, Predicate<K>, Publisher<E>> publisherFunction) {
      Map.Entry<Boolean, Map<Object, Modification>> entryModifications = flattenModificationMaps();

      Map<Object, Modification> modificationCopy = entryModifications.getValue();

      Flowable<E> modPublisher = Flowable.fromIterable(modificationCopy.values())
            .ofType(PutModification.class)
            .filter(modification -> segments.contains(modification.getSegment()))
            .map(putFunction);

      if (filter != null) {
         modPublisher = modPublisher.filter(e -> filter.test(toKeyFunction.apply(e)));
      }

      // We had a clear so skip actually asking the store
      if (entryModifications.getKey()) {
         if (log.isTraceEnabled()) {
            log.trace("Only utilizing pending modifications as clear a was found");
         }
         return modPublisher;
      }

      Predicate<K> combinedPredicate = k -> !modificationCopy.containsKey(k);
      if (filter != null) {
         combinedPredicate = combinedPredicate.and(filter);
      }

      return modPublisher.concatWith(publisherFunction.apply(segments, combinedPredicate));
   }

   private Map.Entry<Boolean, Map<Object, Modification>> flattenModificationMaps() {
      Map<Object, Modification> modificationCopy;
      Map<Object, Modification> modificationsToReplicate;
      boolean clearToReplicate;
      synchronized (this) {
         modificationCopy = new HashMap<>(pendingModifications);
         if (hasPendingClear) {
            return new AbstractMap.SimpleImmutableEntry<>(Boolean.TRUE, modificationCopy);
         }
         modificationsToReplicate = this.replicatingModifications;
         clearToReplicate = this.isReplicatingClear;
      }

      modificationCopy.putAll(modificationsToReplicate);
      return new AbstractMap.SimpleImmutableEntry<>(clearToReplicate, modificationCopy);
   }

   @Override
   public CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
      assertNotStopped();
      CompletionStage<MarshallableEntry<K, V>> pendingStage = getStageFromPending(key);
      if (pendingStage != null) {
         return pendingStage;
      }
      return actual.load(segment, key);
   }

   private CompletionStage<MarshallableEntry<K, V>> getStageFromPending(Object key) {
      Object wrappedKey = wrapKeyIfNeeded(key);
      Map<Object, Modification> modificationsToReplicate;
      boolean clearToReplicate;
      synchronized (this) {
         // Note that writes to this map are done only in synchronized block, so we have to do same for get
         Modification modification = pendingModifications.get(wrappedKey);
         if (modification != null) {
            if (log.isTraceEnabled()) {
               log.tracef("Found entry was pending write in async store: %s", modification);
            }
            return modification.asStage();
         }
         if (hasPendingClear) {
            if (log.isTraceEnabled()) {
               log.trace("There is a pending clear from async store, returning null");
            }
            return CompletableFutures.completedNull();
         }
         // This map is never written to so just reading reference in synchronized block is sufficient
         modificationsToReplicate = this.replicatingModifications;
         clearToReplicate = this.isReplicatingClear;
      }
      Modification modification = modificationsToReplicate.get(wrappedKey);
      if (modification != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Found entry was replicating write in async store: %s", modification);
         }
         return modification.asStage();
      } else if (clearToReplicate) {
         if (log.isTraceEnabled()) {
            log.trace("There is a clear being replicated from async store, returning null");
         }
         return CompletableFutures.completedNull();
      }
      return null;
   }

   @Override
   public CompletionStage<Void> batch(int publisherCount, Publisher<SegmentedPublisher<Object>> removePublisher,
         Publisher<SegmentedPublisher<MarshallableEntry<K, V>>> writePublisher) {
      assertNotStopped();
      Completable removeCompletable = Flowable.fromPublisher(removePublisher)
            .flatMapCompletable(sp -> Flowable.fromPublisher(sp)
                  .concatMapCompletable(key -> Completable.fromCompletionStage(submitModification(new RemoveModification(sp.getSegment(), key))), publisherCount));
      Completable modifyCompletable = Flowable.fromPublisher(writePublisher)
            .flatMapCompletable(sp -> Flowable.fromPublisher(sp)
                  .concatMapCompletable(me -> Completable.fromCompletionStage(submitModification(new PutModification(sp.getSegment(), me))), publisherCount));
      return removeCompletable.mergeWith(modifyCompletable)
            .toCompletionStage(null);
   }

   CompletionStage<Void> submitModification(Modification modification) {
      boolean isTraceEnabled = log.isTraceEnabled();
      boolean startNewBatch;
      CompletionStage<Void> submitStage;
      synchronized (this) {
         int previousBatchId;
         if (isTraceEnabled) {
            previousBatchId = System.identityHashCode(replicatingModifications);
            int currentBatchId = System.identityHashCode(pendingModifications);
            log.tracef("Adding modification %s to batch %s", modification, currentBatchId);
         } else {
            previousBatchId = 0;
         }
         modification.apply(this);

         startNewBatch = batchFuture == null;
         if (startNewBatch) {
            batchFuture = new CompletableFuture<>();
         }

         int queueSize = pendingModifications.size() + replicatingModifications.size();
         submitStage = queueSize > modificationQueueSize ? batchFuture : null;
         if (submitStage != null && isTraceEnabled) {
            log.tracef("Too many modifications queued (%d), operation must wait until previous batch %d completes",
                       queueSize, previousBatchId);
         }
      }

      if (startNewBatch) {
         submitTask();
      }
      return submitStage == null ? CompletableFutures.completedNull() :
             submitStage.thenApplyAsync(CompletableFutures.toNullFunction(), nonBlockingExecutor);
   }

   @Override
   public CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> entry) {
      assertNotStopped();
      return submitModification(new PutModification(segment, entry));
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      assertNotStopped();
      // Return null to signal that we don't know if the key exists in the store
      // Use erasure to avoid calling thenApply
      return (CompletionStage)submitModification(new RemoveModification(segment, key));
   }

   @Override
   public CompletionStage<Void> clear() {
      assertNotStopped();
      submitModification(ClearModification.INSTANCE);
      return CompletableFutures.completedNull();
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> purgeExpired() {
      // We assume our modifications aren't expired - so just call actual store
      return Flowable.defer(() -> {
         assertNotStopped();
         return actual.purgeExpired();
      });
   }

   @Override
   public CompletionStage<Void> addSegments(IntSet segments) {
      assertNotStopped();
      return actual.addSegments(segments);
   }

   @Override
   public CompletionStage<Void> removeSegments(IntSet segments) {
      assertNotStopped();
      synchronized (this) {
         pendingModifications.values().removeIf(modification -> segments.contains(modification.getSegment()));
      }
      return actual.removeSegments(segments);
   }

   @Override
   public CompletionStage<Long> size(IntSet segments) {
      assertNotStopped();
      // TODO: technically this is wrong, but the old version did this is it okay?
      return actual.size(segments);
   }

   @Override
   public CompletionStage<Long> approximateSize(IntSet segments) {
      assertNotStopped();
      return actual.approximateSize(segments);
   }

   @Override
   public CompletionStage<Boolean> isAvailable() {
      if (stopped) {
         return CompletableFutures.completedFalse();
      }

      if (asyncConfiguration.failSilently())
         return CompletableFutures.completedTrue();

      CompletionStage<Boolean> superAvailableStage = super.isAvailable();

      return superAvailableStage.thenApply(delegateAvailable -> {
         if (delegateAvailable) {
            CompletableFuture<Void> delegateFuture;
            synchronized (this) {
               delegateFuture = delegateAvailableFuture;
               delegateAvailableFuture = null;
            }
            if (delegateFuture != null) {
               log.debugf("Underlying delegate %s is now available", actual);
               delegateFuture.complete(null);
            }
            return true;
         }

         boolean delegateUnavailable;
         boolean isReplicating;
         int queueSize;
         synchronized (this) {
            isReplicating = !replicatingModifications.isEmpty() || isReplicatingClear;
            queueSize = pendingModifications.size();
            if (delegateUnavailable = delegateAvailableFuture == null) {
               delegateAvailableFuture = new CompletableFuture<>();
            }
         }
         if (delegateUnavailable) {
            log.debugf("Underlying delegate %s is now unavailable!", actual);
         }
         return queueSize < modificationQueueSize || !isReplicating;
      });
   }

   @Override
   public NonBlockingStore<K, V> delegate() {
      return actual;
   }

   private void assertNotStopped() throws CacheException {
      if (stopped)
         throw new IllegalLifecycleStateException("AsyncCacheWriter stopped; no longer accepting more entries.");
   }

   /**
    * Wraps the provided key if necessary to provide equals to work properly
    * @param key the key to wrap
    * @return the wrapped object (if required) or the object itself
    */
   static Object wrapKeyIfNeeded(Object key) {
      if (key instanceof byte[]) {
         return new WrappedByteArray((byte[]) key);
      }
      return key;
   }
}
