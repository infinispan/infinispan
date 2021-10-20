package org.infinispan.reactive.publisher.impl;

import java.util.HashSet;
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
import org.infinispan.reactive.publisher.impl.commands.reduction.PublisherResult;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.processors.UnicastProcessor;

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

   static <K> Flowable<K> keyPublisherFromContext(InvocationContext ctx, IntSet segments, KeyPartitioner keyPartitioner,
         Set<K> keysToInclude) {
      UnicastProcessor<K> unicastProcessor = UnicastProcessor.create(ctx.lookedUpEntriesCount());
      ctx.forEachValue((o, cacheEntry) -> {
         K key = (K) o;
         if ((keysToInclude == null || keysToInclude.contains(key))
               && (segments == null || segments.contains(keyPartitioner.getSegment(key)))) {
            unicastProcessor.onNext(key);
         }
      });
      unicastProcessor.onComplete();
      return unicastProcessor;
   }

   static <K, V> Flowable<CacheEntry<K, V>> entryPublisherFromContext(InvocationContext ctx, IntSet segments,
         KeyPartitioner keyPartitioner, Set<K> keysToInclude) {
      UnicastProcessor<CacheEntry<K, V>> unicastProcessor = UnicastProcessor.create(ctx.lookedUpEntriesCount());
      ctx.forEachValue((o, cacheEntry) -> {
         K key = (K) o;
         if ((keysToInclude == null || keysToInclude.contains(key))
            && (segments == null || segments.contains(keyPartitioner.getSegment(key)))) {
            unicastProcessor.onNext(cacheEntry);
         }
      });
      unicastProcessor.onComplete();
      return unicastProcessor;
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
      Set<K> keysToExclude = new HashSet<>(invocationContext.lookedUpEntriesCount());
      invocationContext.forEachEntry((key, ce) -> keysToExclude.add((K) key));

      CompletionStage<R> stage = localPublisherManager.keyReduction(parallelPublisher, handleNullSegments(segments), keysToInclude,
            keysToExclude, includeLoader, DeliveryGuarantee.AT_MOST_ONCE, transformer, finalizer).thenApply(PublisherResult::getResult);

      Flowable<K> entryFlowable = keyPublisherFromContext(invocationContext, segments, keyPartitioner, keysToInclude);
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
      Set<K> keysToExclude = new HashSet<>(invocationContext.lookedUpEntriesCount());
      invocationContext.forEachEntry((key, ce) -> keysToExclude.add((K) key));

      CompletionStage<R> stage = localPublisherManager.entryReduction(parallelPublisher, handleNullSegments(segments), keysToInclude,
            keysToExclude, includeLoader, DeliveryGuarantee.AT_MOST_ONCE, transformer, finalizer).thenApply(PublisherResult::getResult);

      Flowable<CacheEntry<K, V>> entryFlowable = entryPublisherFromContext(invocationContext, segments, keyPartitioner,
            keysToInclude);
      return transformer.apply(entryFlowable)
            .thenCombine(stage, Flowable::just)
            .thenCompose(finalizer);
   }

   @Override
   public <R> SegmentCompletionPublisher<R> keyPublisher(IntSet segments, Set<K> keysToInclude,
         InvocationContext invocationContext, boolean includeLoader, DeliveryGuarantee deliveryGuarantee,
         int batchSize, Function<? super Publisher<K>, ? extends Publisher<R>> transformer) {
      if (transformer instanceof InjectableComponent) {
         ((InjectableComponent) transformer).inject(componentRegistry);
      }
      if (invocationContext == null || invocationContext.lookedUpEntriesCount() == 0) {
         return localPublisherManager.keyPublisher(handleNullSegments(segments), keysToInclude, null, includeLoader,
               DeliveryGuarantee.AT_MOST_ONCE, transformer);
      }
      SegmentCompletionPublisher<R> cachePublisher = localPublisherManager.keyPublisher(handleNullSegments(segments),
            keysToInclude, null, includeLoader, DeliveryGuarantee.AT_MOST_ONCE, transformer);

      Flowable<K> keyFlowable = keyPublisherFromContext(invocationContext, segments, keyPartitioner, keysToInclude);

      return new SegmentCompletionPublisher<R>() {
         @Override
         public void subscribeWithSegments(Subscriber<? super Notification<R>> subscriber) {
            Publisher<Notification<R>> cacheData = cachePublisher::subscribeWithSegments;
            Flowable.concat(
                        Flowable.fromPublisher(transformer.apply(keyFlowable))
                              .map(Notifications::value),
                        cacheData)
                  .subscribe(subscriber);
         }

         @Override
         public void subscribe(Subscriber<? super R> s) {
            Flowable.concat(
                        transformer.apply(keyFlowable),
                        cachePublisher)
                  .subscribe(s);
         }
      };
   }

   @Override
   public <R> SegmentCompletionPublisher<R> entryPublisher(IntSet segments, Set<K> keysToInclude,
         InvocationContext invocationContext, boolean includeLoader, DeliveryGuarantee deliveryGuarantee, int batchSize,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends Publisher<R>> transformer) {
      if (transformer instanceof InjectableComponent) {
         ((InjectableComponent) transformer).inject(componentRegistry);
      }
      if (invocationContext == null || invocationContext.lookedUpEntriesCount() == 0) {
         return localPublisherManager.entryPublisher(handleNullSegments(segments), keysToInclude, null, includeLoader,
               DeliveryGuarantee.AT_MOST_ONCE, transformer);
      }
      SegmentCompletionPublisher<R> cachePublisher = localPublisherManager.entryPublisher(handleNullSegments(segments), keysToInclude, null,
            includeLoader, DeliveryGuarantee.AT_MOST_ONCE, transformer);

      Flowable<CacheEntry<K, V>> entryFlowable = entryPublisherFromContext(invocationContext, segments, keyPartitioner,
            keysToInclude);

      return new SegmentCompletionPublisher<R>() {
         @Override
         public void subscribeWithSegments(Subscriber<? super Notification<R>> subscriber) {
            Publisher<Notification<R>> cacheData = cachePublisher::subscribeWithSegments;
            Flowable.concat(
                        Flowable.fromPublisher(transformer.apply(entryFlowable))
                              .map(Notifications::value),
                        cacheData)
                  .subscribe(subscriber);
         }

         @Override
         public void subscribe(Subscriber<? super R> s) {
            Flowable.concat(
                        transformer.apply(entryFlowable),
                        cachePublisher)
                  .subscribe(s);
         }
      };
   }
}
