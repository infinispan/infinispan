package org.infinispan.persistence.support;

import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import javax.transaction.Transaction;

import org.infinispan.commons.util.IntSet;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.test.AbstractInfinispanTest;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;

public abstract class EnsureNonBlockingStore<K, V> extends DelegatingNonBlockingStore<K, V> implements WaitNonBlockingStore<K, V> {
   @Override
   public CompletionStage<Void> start(InitializationContext ctx) {
      return AbstractInfinispanTest.ensureNonBlocking(() -> delegate().start(ctx));
   }

   @Override
   public CompletionStage<Void> stop() {
      return AbstractInfinispanTest.ensureNonBlocking(() -> delegate().stop());
   }

   @Override
   public CompletionStage<Boolean> isAvailable() {
      return AbstractInfinispanTest.ensureNonBlocking(() -> delegate().isAvailable());
   }

   @Override
   public CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
      return AbstractInfinispanTest.ensureNonBlocking(() -> delegate().load(segment, key));
   }

   @Override
   public CompletionStage<Boolean> containsKey(int segment, Object key) {
      return AbstractInfinispanTest.ensureNonBlocking(() -> delegate().containsKey(segment, key));
   }

   @Override
   public CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> entry) {
      return AbstractInfinispanTest.ensureNonBlocking(() -> delegate().write(segment, entry));
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      return AbstractInfinispanTest.ensureNonBlocking(() -> delegate().delete(segment, key));
   }

   @Override
   public CompletionStage<Void> addSegments(IntSet segments) {
      return AbstractInfinispanTest.ensureNonBlocking(() -> delegate().addSegments(segments));
   }

   @Override
   public CompletionStage<Void> removeSegments(IntSet segments) {
      return AbstractInfinispanTest.ensureNonBlocking(() -> delegate().removeSegments(segments));
   }

   @Override
   public CompletionStage<Void> clear() {
      return AbstractInfinispanTest.ensureNonBlocking(delegate()::clear);
   }

   @Override
   public CompletionStage<Void> bulkWrite(int publisherCount, Publisher<SegmentedPublisher<MarshallableEntry<K, V>>> publisher) {
      return AbstractInfinispanTest.ensureNonBlocking(() -> delegate().bulkWrite(publisherCount, publisher));
   }

   @Override
   public CompletionStage<Void> bulkDelete(int publisherCount, Publisher<SegmentedPublisher<Object>> publisher) {
      return AbstractInfinispanTest.ensureNonBlocking(() -> delegate().bulkDelete(publisherCount, publisher));
   }

   @Override
   public CompletionStage<Long> size(IntSet segments) {
      return AbstractInfinispanTest.ensureNonBlocking(() -> delegate().size(segments));
   }

   @Override
   public CompletionStage<Long> approximateSize(IntSet segments) {
      return AbstractInfinispanTest.ensureNonBlocking(() -> delegate().approximateSize(segments));
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean includeValues) {
      return AbstractInfinispanTest.ensureNonBlocking(() ->
         Flowable.fromPublisher(delegate().publishEntries(segments, filter, includeValues))
               .subscribeOn(Schedulers.from(AbstractInfinispanTest.ensureNonBlockingExecutor()))
      );
   }

   @Override
   public Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
      return AbstractInfinispanTest.ensureNonBlocking(() ->
            Flowable.fromPublisher(delegate().publishKeys(segments, filter))
                  .subscribeOn(Schedulers.from(AbstractInfinispanTest.ensureNonBlockingExecutor()))
      );
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> purgeExpired() {
      return AbstractInfinispanTest.ensureNonBlocking(() -> delegate().purgeExpired());
   }

   @Override
   public CompletionStage<Void> prepareWithModifications(Transaction transaction, BatchModification batchModification) {
      return AbstractInfinispanTest.ensureNonBlocking(() -> delegate().prepareWithModifications(transaction, batchModification));
   }

   @Override
   public CompletionStage<Void> commit(Transaction transaction) {
      return AbstractInfinispanTest.ensureNonBlocking(() -> delegate().commit(transaction));
   }

   @Override
   public CompletionStage<Void> rollback(Transaction transaction) {
      return AbstractInfinispanTest.ensureNonBlocking(() -> delegate().rollback(transaction));
   }
}
