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
import org.reactivestreams.Subscriber;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.processors.AsyncProcessor;
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
   public <R> SegmentCompletionPublisher<R> keyPublisher(IntSet segments, Set<K> keysToInclude,
         InvocationContext invocationContext, boolean includeLoader, DeliveryGuarantee deliveryGuarantee, int batchSize,
         Function<? super Publisher<K>, ? extends Publisher<R>> transformer) {
      checkPartitionStatus();
      SegmentCompletionPublisher<R> original = super.keyPublisher(segments, keysToInclude, invocationContext,
            includeLoader, deliveryGuarantee, batchSize, transformer);
      return registerPublisher(original);
   }

   @Override
   public <R> SegmentCompletionPublisher<R> entryPublisher(IntSet segments, Set<K> keysToInclude,
         InvocationContext invocationContext, boolean includeLoader, DeliveryGuarantee deliveryGuarantee, int batchSize,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends Publisher<R>> transformer) {
      checkPartitionStatus();
      SegmentCompletionPublisher<R> original = super.entryPublisher(segments, keysToInclude, invocationContext,
            includeLoader, deliveryGuarantee, batchSize, transformer);
      return registerPublisher(original);
   }

   private <R> SegmentCompletionPublisher<R> registerPublisher(SegmentCompletionPublisher<R> original) {
      return new SegmentCompletionPublisher<R>() {
         @Override
         public void subscribeWithSegments(Subscriber<? super Notification<R>> subscriber) {
            handleEarlyTermination(subscriber, scp -> Flowable.<Notification<R>>fromPublisher(scp::subscribeWithSegments));
         }

         @Override
         public void subscribe(Subscriber<? super R> subscriber) {
            handleEarlyTermination(subscriber, Flowable::fromPublisher);
         }

         private <S> void handleEarlyTermination(Subscriber<? super S> subscriber, Function<SegmentCompletionPublisher<R>, Flowable<S>> function) {
            // Processor has to be serialized due to possibly invoking onError from a different thread
            FlowableProcessor<S> earlyTerminatingProcessor = AsyncProcessor.<S>create().toSerialized();
            pendingProcessors.add(earlyTerminatingProcessor);

            // Have to check after registering in case if we got a partition between when the publisher was created and
            // subscribed to
            if (isPartitionDegraded()) {
               pendingProcessors.remove(earlyTerminatingProcessor);
               earlyTerminatingProcessor.onError(CLUSTER.partitionDegraded());
               // There are no types here so cast is fine
               // noinspection unchecked
               original.subscribe((Subscriber<? super R>) earlyTerminatingProcessor);
            } else {
               Flowable<S> actualPublisher = function.apply(original)
                     .doFinally(earlyTerminatingProcessor::onComplete);
               Publisher<S> flowableAsPublisher = earlyTerminatingProcessor
                     .doFinally(() -> pendingProcessors.remove(earlyTerminatingProcessor));
               Flowable.merge(flowableAsPublisher, actualPublisher)
                     .subscribe(subscriber);
            }
         }
      };
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
