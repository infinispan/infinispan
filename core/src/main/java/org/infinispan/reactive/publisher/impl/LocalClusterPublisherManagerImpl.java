package org.infinispan.reactive.publisher.impl;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.reactive.RxJavaInterop;
import org.infinispan.reactive.publisher.impl.commands.reduction.PublisherResult;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

@Scope(Scopes.NAMED_CACHE)
public class LocalClusterPublisherManagerImpl<K, V> implements ClusterPublisherManager<K, V> {
   @Inject LocalPublisherManager<K, V> localPublisherManager;
   @Inject Configuration cacheConfiguration;
   @Inject KeyPartitioner keyPartitioner;
   @Inject ComponentRegistry componentRegistry;

   private int maxSegment;

   @Start
   public void start() {
      if (Configurations.needSegments(cacheConfiguration)) {
         maxSegment = cacheConfiguration.clustering().hash().numSegments();
      } else {
         maxSegment = 1;
      }
   }

   static <K, V> Flowable<CacheEntry<K, V>> entryPublisherFromContext(InvocationContext ctx, IntSet segments,
         KeyPartitioner keyPartitioner, Set<K> keysToInclude) {
      Flowable<CacheEntry<K, V>> flowable = Flowable.fromPublisher(ctx.publisher());
      if (segments == null && keysToInclude == null) {
         return flowable;
      }
      return flowable.filter(entry -> (keysToInclude == null || keysToInclude.contains(entry.getKey()))
            && (segments == null || segments.contains(keyPartitioner.getSegment(entry.getKey()))));
   }

   static <K, V> Flowable<SegmentPublisherSupplier.Notification<CacheEntry<K, V>>> notificationPublisherFromContext(
         InvocationContext ctx, IntSet segments, KeyPartitioner keyPartitioner, Set<K> keysToInclude) {
      return Flowable.fromPublisher(ctx.<K, V>publisher())
            .mapOptional(ce -> {
               K key = ce.getKey();
               if (keysToInclude == null || keysToInclude.contains(key)) {
                  int segment = keyPartitioner.getSegment(key);
                  if (segments == null || segments.contains(segment)) {
                     return Optional.of(Notifications.value(ce, segment));
                  }
               }
               return Optional.empty();
            });
   }

   IntSet handleNullSegments(IntSet segments) {
      return segments != null ? segments : IntSets.immutableRangeSet(maxSegment);
   }

   @Override
   public <R> CompletionStage<R> keyReduction(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
         InvocationContext invocationContext, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      if (transformer instanceof InjectableComponent) {
         ((InjectableComponent) transformer).inject(componentRegistry);
      }
      if (finalizer instanceof InjectableComponent) {
         ((InjectableComponent) finalizer).inject(componentRegistry);
      }
      if (invocationContext == null || invocationContext.lookedUpEntriesCount() == 0) {
         return localPublisherManager.keyReduction(parallelPublisher, handleNullSegments(segments), keysToInclude, null,
               includeLoader, DeliveryGuarantee.AT_MOST_ONCE, transformer, finalizer).thenApply(PublisherResult::getResult);
      }

      CompletionStage<R> stage = localPublisherManager.keyReduction(parallelPublisher, handleNullSegments(segments), keysToInclude,
                  (Set<K>) invocationContext.getLookedUpEntries().keySet(), includeLoader, DeliveryGuarantee.AT_MOST_ONCE, transformer, finalizer)
            .thenApply(PublisherResult::getResult);

      Flowable<K> entryFlowable = entryPublisherFromContext(invocationContext, segments, keyPartitioner, keysToInclude)
            .map(RxJavaInterop.entryToKeyFunction());
      return transformer.apply(entryFlowable)
            .thenCombine(stage, Flowable::just)
            .thenCompose(finalizer);
   }

   @Override
   public <R> CompletionStage<R> entryReduction(boolean parallelPublisher, IntSet segments, Set<K> keysToInclude,
         InvocationContext invocationContext, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      if (transformer instanceof InjectableComponent) {
         ((InjectableComponent) transformer).inject(componentRegistry);
      }
      if (finalizer instanceof InjectableComponent) {
         ((InjectableComponent) finalizer).inject(componentRegistry);
      }
      if (invocationContext == null || invocationContext.lookedUpEntriesCount() == 0) {
         return localPublisherManager.entryReduction(parallelPublisher, handleNullSegments(segments), keysToInclude, null,
               includeLoader, DeliveryGuarantee.AT_MOST_ONCE, transformer, finalizer).thenApply(PublisherResult::getResult);
      }

      CompletionStage<R> stage = localPublisherManager.entryReduction(parallelPublisher, handleNullSegments(segments), keysToInclude,
                  (Set<K>) invocationContext.getLookedUpEntries().keySet(), includeLoader, DeliveryGuarantee.AT_MOST_ONCE, transformer, finalizer)
            .thenApply(PublisherResult::getResult);

      Flowable<CacheEntry<K, V>> entryFlowable = entryPublisherFromContext(invocationContext, segments, keyPartitioner,
            keysToInclude);
      return transformer.apply(entryFlowable)
            .thenCombine(stage, Flowable::just)
            .thenCompose(finalizer);
   }

   @Override
   public <R> SegmentPublisherSupplier<R> keyPublisher(IntSet segments, Set<K> keysToInclude,
         InvocationContext invocationContext, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         int batchSize, Function<? super Publisher<K>, ? extends Publisher<R>> transformer) {
      if (transformer instanceof InjectableComponent) {
         ((InjectableComponent) transformer).inject(componentRegistry);
      }
      if (invocationContext == null || invocationContext.lookedUpEntriesCount() == 0) {
         return localPublisherManager.keyPublisher(handleNullSegments(segments),
               keysToInclude, null, includeLoader, DeliveryGuarantee.AT_MOST_ONCE, transformer);
      }

      SegmentAwarePublisherSupplier<R> cachePublisher = localPublisherManager.keyPublisher(handleNullSegments(segments),
            keysToInclude, (Set<K>) invocationContext.getLookedUpEntries().keySet(), includeLoader, DeliveryGuarantee.AT_MOST_ONCE, transformer);

      return new SegmentPublisherSupplier<R>() {
         @Override
         public Publisher<Notification<R>> publisherWithSegments() {
            Flowable<Notification<CacheEntry<K, V>>> contextFlowable =
                  notificationPublisherFromContext(invocationContext, segments, keyPartitioner, keysToInclude);

            return Flowable.concat(contextFlowable.concatMap(notification ->
                        Flowable.fromPublisher(transformer.apply(Flowable.just(notification.value().getKey())))
                              .map(r -> Notifications.value(r, notification.valueSegment()))),
                  cachePublisher.publisherWithSegments());
         }

         @Override
         public Publisher<R> publisherWithoutSegments() {
            Flowable<K> contextFlowable = entryPublisherFromContext(invocationContext, segments, keyPartitioner, keysToInclude)
                  .map(RxJavaInterop.entryToKeyFunction());
            return Flowable.concat(transformer.apply(contextFlowable),
                  cachePublisher.publisherWithoutSegments());
         }
      };
   }

   @Override
   public <R> SegmentPublisherSupplier<R> entryPublisher(IntSet segments, Set<K> keysToInclude,
         InvocationContext invocationContext, boolean includeLoader, DeliveryGuarantee deliveryGuarantee, int batchSize,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends Publisher<R>> transformer) {
      if (transformer instanceof InjectableComponent) {
         ((InjectableComponent) transformer).inject(componentRegistry);
      }

      if (invocationContext == null || invocationContext.lookedUpEntriesCount() == 0) {
         return localPublisherManager.entryPublisher(handleNullSegments(segments),
               keysToInclude, null, includeLoader, DeliveryGuarantee.AT_MOST_ONCE, transformer);
      }

      SegmentAwarePublisherSupplier<R> cachePublisher = localPublisherManager.entryPublisher(handleNullSegments(segments),
            keysToInclude, (Set<K>) invocationContext.getLookedUpEntries().keySet(), includeLoader, DeliveryGuarantee.AT_MOST_ONCE, transformer);

      return new SegmentPublisherSupplier<R>() {

         @Override
         public Publisher<Notification<R>> publisherWithSegments() {
            Flowable<Notification<CacheEntry<K, V>>> entryFlowable = notificationPublisherFromContext(invocationContext, segments, keyPartitioner,
                  keysToInclude);

            Flowable<Notification<R>> contextFlowable = entryFlowable
                  .concatMap(notification -> Flowable.fromPublisher(transformer.apply(Flowable.just(notification.value())))
                        .map(r -> Notifications.value(r, notification.valueSegment())));

            return Flowable.concat(contextFlowable,
                  cachePublisher.publisherWithSegments());
         }

         @Override
         public Publisher<R> publisherWithoutSegments() {
            Flowable<CacheEntry<K, V>> entryFlowable = entryPublisherFromContext(invocationContext, segments, keyPartitioner,
                  keysToInclude);
            return Flowable.concat(transformer.apply(entryFlowable),
                  cachePublisher.publisherWithoutSegments());
         }
      };
   }
}
