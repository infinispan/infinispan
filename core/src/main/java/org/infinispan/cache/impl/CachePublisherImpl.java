package org.infinispan.cache.impl;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.CachePublisher;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.factories.PublisherManagerFactory;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManager;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.reactive.publisher.impl.SegmentPublisherSupplier;
import org.infinispan.util.function.SerializableFunction;
import org.reactivestreams.Publisher;

public class CachePublisherImpl<K, V> implements CachePublisher<K, V> {
   private final ClusterPublisherManager<K, V> clusterPublisherManager;
   private final InvocationContextFactory invocationContextFactory;
   private final long flags;

   private final boolean parallel;
   private final int batchSize;
   private final Set<K> keys;
   private final IntSet segments;
   private final DeliveryGuarantee guarantee;

   public CachePublisherImpl(CacheImpl<K, V> cache, long flags) {
      if (EnumUtil.containsAll(flags, FlagBitSets.CACHE_MODE_LOCAL)) {
         clusterPublisherManager = cache.componentRegistry.getComponent(ClusterPublisherManager.class,
               PublisherManagerFactory.LOCAL_CLUSTER_PUBLISHER);
      } else {
         clusterPublisherManager = cache.componentRegistry.getComponent(ClusterPublisherManager.class);
      }
      this.invocationContextFactory = cache.invocationContextFactory;
      this.flags = flags;

      this.parallel = false;
      this.batchSize = cache.getCacheConfiguration().clustering().stateTransfer().chunkSize();
      this.keys = null;
      this.segments = null;
      this.guarantee = DeliveryGuarantee.EXACTLY_ONCE;
   }

   public CachePublisherImpl(ClusterPublisherManager<K, V> clusterPublisherManager) {
      this.clusterPublisherManager = clusterPublisherManager;
      this.invocationContextFactory = null;
      this.flags = EnumUtil.EMPTY_BIT_SET;

      this.parallel = false;
      this.batchSize = 0;
      this.keys = null;
      this.segments = null;
      this.guarantee = DeliveryGuarantee.EXACTLY_ONCE;
   }

   private CachePublisherImpl(CachePublisherImpl<K, V> other, boolean parallel, int batchSize,
                              Set<K> keys, IntSet segments, DeliveryGuarantee guarantee) {
      this.clusterPublisherManager = other.clusterPublisherManager;
      this.invocationContextFactory = other.invocationContextFactory;
      this.flags = other.flags;

      this.parallel = parallel;
      this.batchSize = batchSize;
      this.keys = keys;
      this.segments = segments;
      this.guarantee = guarantee;
   }

   @Override
   public CachePublisher<K, V> parallelReduction() {
      if (this.parallel) {
         return this;
      }
      return new CachePublisherImpl<>(this, true, this.batchSize, this.keys, this.segments, this.guarantee);
   }

   @Override
   public CachePublisher<K, V> sequentialReduction() {
      if (!this.parallel) {
         return this;
      }
      return new CachePublisherImpl<>(this, false, this.batchSize, this.keys, this.segments, this.guarantee);
   }

   @Override
   public CachePublisher<K, V> batchSize(int batchSize) {
      if (batchSize <= 0) {
         throw new IllegalArgumentException("batchSize must be greater than 0 was " + batchSize);
      }
      if (this.batchSize == batchSize) {
         return this;
      }
      return new CachePublisherImpl<>(this, this.parallel, batchSize, this.keys, this.segments, this.guarantee);
   }

   @Override
   public CachePublisher<K, V> withKeys(Set<? extends K> keys) {
      return new CachePublisherImpl<>(this, this.parallel, this.batchSize, (Set<K>) Objects.requireNonNull(keys),
            this.segments, this.guarantee);
   }

   @Override
   public CachePublisher<K, V> withAllKeys() {
      if (keys == null) {
         return this;
      }
      return new CachePublisherImpl<>(this, this.parallel, this.batchSize, null, this.segments, this.guarantee);
   }

   @Override
   public CachePublisher<K, V> withSegments(IntSet segments) {
      return new CachePublisherImpl<>(this, this.parallel, this.batchSize, this.keys,
            Objects.requireNonNull(segments), this.guarantee);
   }

   @Override
   public CachePublisher<K, V> withAllSegments() {
      if (this.segments == null) {
         return this;
      }
      return new CachePublisherImpl<>(this, this.parallel, this.batchSize, this.keys, null, this.guarantee);
   }

   @Override
   public CachePublisher<K, V> atMostOnce() {
      return forGuarantee(DeliveryGuarantee.AT_MOST_ONCE);
   }

   @Override
   public CachePublisher<K, V> atLeastOnce() {
      return forGuarantee(DeliveryGuarantee.AT_LEAST_ONCE);
   }

   @Override
   public CachePublisher<K, V> exactlyOnce() {
      return forGuarantee(DeliveryGuarantee.EXACTLY_ONCE);
   }

   private CachePublisher<K, V> forGuarantee(DeliveryGuarantee guarantee) {
      if (this.guarantee == guarantee) {
         return this;
      }
      return new CachePublisherImpl<>(this, this.parallel, this.batchSize, this.keys, this.segments,
            Objects.requireNonNull(guarantee));
   }

   @Override
   public <R> CompletionStage<R> keyReduction(Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer, Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      InvocationContext ctx = invocationContextFactory != null ? invocationContextFactory.createInvocationContext(false, InvocationContextFactory.UNBOUNDED) : null;
      return clusterPublisherManager.keyReduction(parallel, segments, keys, ctx, flags, guarantee,
            Objects.requireNonNull(transformer), Objects.requireNonNull(finalizer));
   }

   @Override
   public <R> CompletionStage<R> keyReduction(SerializableFunction<? super Publisher<K>, ? extends CompletionStage<R>> transformer, SerializableFunction<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      return keyReduction((Function<? super Publisher<K>, ? extends CompletionStage<R>>) transformer, finalizer);
   }

   @Override
   public <R> CompletionStage<R> entryReduction(Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer, Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      InvocationContext ctx = invocationContextFactory != null ? invocationContextFactory.createInvocationContext(false, InvocationContextFactory.UNBOUNDED) : null;
      return clusterPublisherManager.entryReduction(parallel, segments, keys, ctx, flags, guarantee,
            Objects.requireNonNull(transformer), Objects.requireNonNull(finalizer));
   }

   @Override
   public <R> CompletionStage<R> entryReduction(SerializableFunction<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer, SerializableFunction<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      return entryReduction((Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>>) transformer, finalizer);
   }

   @Override
   public <R> SegmentPublisherSupplier<R> keyPublisher(Function<? super Publisher<K>, ? extends Publisher<R>> transformer) {
      InvocationContext ctx = invocationContextFactory != null ? invocationContextFactory.createInvocationContext(false, InvocationContextFactory.UNBOUNDED) : null;
      return clusterPublisherManager.keyPublisher(segments, keys, ctx, flags, guarantee, batchSize,
            Objects.requireNonNull(transformer));
   }

   @Override
   public <R> SegmentPublisherSupplier<R> keyPublisher(SerializableFunction<? super Publisher<K>, ? extends Publisher<R>> transformer) {
      return keyPublisher((Function<? super Publisher<K>, ? extends Publisher<R>>) transformer);
   }

   @Override
   public <R> SegmentPublisherSupplier<R> entryPublisher(Function<? super Publisher<CacheEntry<K, V>>, ? extends Publisher<R>> transformer) {
      InvocationContext ctx = invocationContextFactory != null ? invocationContextFactory.createInvocationContext(false, InvocationContextFactory.UNBOUNDED) : null;
      return clusterPublisherManager.entryPublisher(segments, keys, ctx, flags, guarantee, batchSize,
            Objects.requireNonNull(transformer));
   }

   @Override
   public <R> SegmentPublisherSupplier<R> entryPublisher(SerializableFunction<? super Publisher<CacheEntry<K, V>>, ? extends Publisher<R>> transformer) {
      return entryPublisher((Function<? super Publisher<CacheEntry<K, V>>, ? extends Publisher<R>>) transformer);
   }
}
