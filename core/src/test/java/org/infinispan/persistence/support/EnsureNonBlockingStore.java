package org.infinispan.persistence.support;

import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import javax.transaction.Transaction;

import org.infinispan.commons.test.BlockHoundHelper;
import org.infinispan.commons.util.IntSet;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;

public class EnsureNonBlockingStore<K, V> extends WaitDelegatingNonBlockingStore<K, V> {
   public EnsureNonBlockingStore(NonBlockingStore<K, V> delegate, KeyPartitioner keyPartitioner) {
      super(delegate, keyPartitioner);
   }

   @Override
   public CompletionStage<Void> start(InitializationContext ctx) {
      return BlockHoundHelper.ensureNonBlocking(() -> delegate().start(ctx));
   }

   @Override
   public CompletionStage<Void> stop() {
      return BlockHoundHelper.ensureNonBlocking(() -> delegate().stop());
   }

   @Override
   public CompletionStage<Boolean> isAvailable() {
      return BlockHoundHelper.ensureNonBlocking(() -> delegate().isAvailable());
   }

   @Override
   public CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
      return BlockHoundHelper.ensureNonBlocking(() -> delegate().load(segment, key));
   }

   @Override
   public CompletionStage<Boolean> containsKey(int segment, Object key) {
      return BlockHoundHelper.ensureNonBlocking(() -> delegate().containsKey(segment, key));
   }

   @Override
   public CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> entry) {
      return BlockHoundHelper.ensureNonBlocking(() -> delegate().write(segment, entry));
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      return BlockHoundHelper.ensureNonBlocking(() -> delegate().delete(segment, key));
   }

   @Override
   public CompletionStage<Void> addSegments(IntSet segments) {
      return BlockHoundHelper.ensureNonBlocking(() -> delegate().addSegments(segments));
   }

   @Override
   public CompletionStage<Void> removeSegments(IntSet segments) {
      return BlockHoundHelper.ensureNonBlocking(() -> delegate().removeSegments(segments));
   }

   @Override
   public CompletionStage<Void> clear() {
      return BlockHoundHelper.ensureNonBlocking(delegate()::clear);
   }

   @Override
   public CompletionStage<Void> batch(int publisherCount, Publisher<NonBlockingStore.SegmentedPublisher<Object>> removePublisher,
         Publisher<NonBlockingStore.SegmentedPublisher<MarshallableEntry<K, V>>> writePublisher) {
      return BlockHoundHelper.ensureNonBlocking(() -> delegate().batch(publisherCount, removePublisher, writePublisher));
   }

   @Override
   public CompletionStage<Long> size(IntSet segments) {
      return BlockHoundHelper.ensureNonBlocking(() -> delegate().size(segments));
   }

   @Override
   public CompletionStage<Long> approximateSize(IntSet segments) {
      return BlockHoundHelper.ensureNonBlocking(() -> delegate().approximateSize(segments));
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean includeValues) {
      return BlockHoundHelper.ensureNonBlocking(() ->
         Flowable.fromPublisher(delegate().publishEntries(segments, filter, includeValues))
               .subscribeOn(Schedulers.from(BlockHoundHelper.ensureNonBlockingExecutor()))
      );
   }

   @Override
   public Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
      return BlockHoundHelper.ensureNonBlocking(() ->
            Flowable.fromPublisher(delegate().publishKeys(segments, filter))
                  .subscribeOn(Schedulers.from(BlockHoundHelper.ensureNonBlockingExecutor()))
      );
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> purgeExpired() {
      return BlockHoundHelper.ensureNonBlocking(() -> delegate().purgeExpired());
   }

   @Override
   public CompletionStage<Void> prepareWithModifications(Transaction transaction, int publisherCount,
         Publisher<SegmentedPublisher<Object>> removePublisher,
         Publisher<SegmentedPublisher<MarshallableEntry<K, V>>> writePublisher) {
      return BlockHoundHelper.ensureNonBlocking(() -> delegate().prepareWithModifications(transaction, publisherCount,
            removePublisher, writePublisher));
   }

   @Override
   public CompletionStage<Void> commit(Transaction transaction) {
      return BlockHoundHelper.ensureNonBlocking(() -> delegate().commit(transaction));
   }

   @Override
   public CompletionStage<Void> rollback(Transaction transaction) {
      return BlockHoundHelper.ensureNonBlocking(() -> delegate().rollback(transaction));
   }
}
