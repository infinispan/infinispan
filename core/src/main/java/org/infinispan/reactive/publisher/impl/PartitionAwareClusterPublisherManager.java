package org.infinispan.reactive.publisher.impl;

import static org.infinispan.util.logging.Log.CLUSTER;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.PartitionStatusChanged;
import org.infinispan.notifications.cachelistener.event.PartitionStatusChangedEvent;
import org.infinispan.partitionhandling.AvailabilityException;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.processors.FlowableProcessor;

/**
 * Cluster stream manager that also pays attention to partition status and properly closes iterators and throws
 * exceptions when the availability mode changes.
 */
@Scope(Scopes.NAMED_CACHE)
public class PartitionAwareClusterPublisherManager<K, V> extends ClusterPublisherManagerImpl<K, V> {
   volatile AvailabilityMode currentMode = AvailabilityMode.AVAILABLE;

   protected final PartitionListener listener = new PartitionListener();
   @Inject protected ComponentRef<Cache<?, ?>> cache;

   private final Set<CompletableFuture<?>> pendingCompletableFutures = ConcurrentHashMap.newKeySet();
   private final Set<FlowableProcessor<?>> pendingProcessors = ConcurrentHashMap.newKeySet();

   @Listener
   private class PartitionListener {
      volatile AvailabilityMode currentMode = AvailabilityMode.AVAILABLE;

      @PartitionStatusChanged
      public void onPartitionChange(PartitionStatusChangedEvent<K, ?> event) {
         if (!event.isPre()) {
            AvailabilityMode newMode = event.getAvailabilityMode();
            if (newMode == AvailabilityMode.DEGRADED_MODE) {
               AvailabilityException ae = CLUSTER.partitionDegraded();
               pendingProcessors.forEach(pp -> pp.onError(ae));
               pendingCompletableFutures.forEach(cf -> cf.completeExceptionally(ae));
            }
            // We have to assign this after reassigning exceptionProcessor if necessary
            currentMode = newMode;
         }
      }
   }

   public void start() {
      super.start();
      cache.running().addListener(listener);
   }

   @Override
   public <R> CompletionStage<R> keyReduction(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
         InvocationContext ctx, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      checkPartitionStatus();
      CompletionStage<R> original = super.keyReduction(parallelPublisher, segments, keysToInclude, ctx, includeLoader,
            deliveryGuarantee, transformer, finalizer);
      return registerStage(original);
   }

   @Override
   public <R> CompletionStage<R> entryReduction(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
         InvocationContext ctx, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      checkPartitionStatus();
      CompletionStage<R> original = super.entryReduction(parallelPublisher, segments, keysToInclude, ctx, includeLoader,
            deliveryGuarantee, transformer, finalizer);
      return registerStage(original);
   }

   private <R> CompletionStage<R> registerStage(CompletionStage<R> original) {
      CompletableFuture<R> future = new CompletableFuture<>();
      pendingCompletableFutures.add(future);
      // Recheck after adding to futures to close small gap between
      if (isPartitionDegraded()) {
         pendingCompletableFutures.remove(future);
         future.completeExceptionally(CLUSTER.partitionDegraded());
      } else {
         original.whenComplete((value, t) -> {
            if (t != null) {
               future.completeExceptionally(t);
            } else {
               future.complete(value);
            }
            pendingCompletableFutures.remove(future);
         });
      }
      return future;
   }

   @Override
   public <R> SegmentPublisherSupplier<R> keyPublisher(IntSet segments, Set<K> keysToInclude,
         InvocationContext invocationContext, boolean includeLoader, DeliveryGuarantee deliveryGuarantee, int batchSize,
         Function<? super Publisher<K>, ? extends Publisher<R>> transformer) {
      checkPartitionStatus();
      SegmentPublisherSupplier<R> original = super.keyPublisher(segments, keysToInclude, invocationContext,
            includeLoader, deliveryGuarantee, batchSize, transformer);
      return registerPublisher(original);
   }

   @Override
   public <R> SegmentPublisherSupplier<R> entryPublisher(IntSet segments, Set<K> keysToInclude,
         InvocationContext invocationContext, boolean includeLoader, DeliveryGuarantee deliveryGuarantee, int batchSize,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends Publisher<R>> transformer) {
      checkPartitionStatus();
      SegmentPublisherSupplier<R> original = super.entryPublisher(segments, keysToInclude, invocationContext,
            includeLoader, deliveryGuarantee, batchSize, transformer);
      return registerPublisher(original);
   }

   private <R> SegmentPublisherSupplier<R> registerPublisher(SegmentPublisherSupplier<R> original) {
      return new SegmentPublisherSupplier<R>() {
         @Override
         public Publisher<Notification<R>> publisherWithSegments() {
            return handleEarlyTermination(SegmentPublisherSupplier::publisherWithSegments);
         }

         @Override
         public Publisher<R> publisherWithoutSegments() {
            return handleEarlyTermination(SegmentPublisherSupplier::publisherWithoutSegments);
         }

         private <S> Flowable<S> handleEarlyTermination(Function<SegmentPublisherSupplier<R>, Publisher<S>> function) {
            CompletableFuture<Void> cf = new CompletableFuture<>();
            pendingCompletableFutures.add(cf);

            return Flowable.fromPublisher(function.apply(original))
                  .doOnNext(s -> throwExceptionIfNeeded(cf))
                  .doOnComplete(() -> throwExceptionIfNeeded(cf))
                  .doFinally(() -> pendingProcessors.remove(cf));
         }
      };
   }

   private void throwExceptionIfNeeded(CompletableFuture<Void> cf) {
      if (cf.isDone())
         cf.join();
   }

   private void checkPartitionStatus() {
      if (isPartitionDegraded()) {
         throw CLUSTER.partitionDegraded();
      }
   }

   private boolean isPartitionDegraded() {
      return currentMode != AvailabilityMode.AVAILABLE;
   }
}
