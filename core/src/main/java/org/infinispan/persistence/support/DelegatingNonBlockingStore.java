package org.infinispan.persistence.support;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import javax.transaction.Transaction;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.IntSet;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.reactivestreams.Publisher;

public abstract class DelegatingNonBlockingStore<K, V> implements NonBlockingStore<K, V> {
   public abstract NonBlockingStore<K, V> delegate();

   @Override
   public CompletionStage<Void> start(InitializationContext ctx) {
      return delegate().start(ctx);
   }

   @Override
   public CompletionStage<Void> stop() {
      return delegate().stop();
   }

   @Override
   public Set<Characteristic> characteristics() {
      return delegate().characteristics();
   }

   @Override
   public MediaType getKeyMediaType(MediaType storageMediaType, Set<MediaType> supportedMediaTypes) {
      return delegate().getKeyMediaType(storageMediaType, supportedMediaTypes);
   }

   @Override
   public MediaType getValueMediaType(MediaType storageMediaType, Set<MediaType> supportedMediaTypes) {
      return delegate().getValueMediaType(storageMediaType, supportedMediaTypes);
   }

   @Override
   public CompletionStage<Boolean> isAvailable() {
      return delegate().isAvailable();
   }

   @Override
   public CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key) {
      return delegate().load(segment, key);
   }

   @Override
   public CompletionStage<Boolean> containsKey(int segment, Object key) {
      return delegate().containsKey(segment, key);
   }

   @Override
   public CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> entry) {
      return delegate().write(segment, entry);
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      return delegate().delete(segment, key);
   }

   @Override
   public CompletionStage<Void> addSegments(IntSet segments) {
      return delegate().addSegments(segments);
   }

   @Override
   public CompletionStage<Void> removeSegments(IntSet segments) {
      return delegate().removeSegments(segments);
   }

   @Override
   public CompletionStage<Void> clear() {
      return delegate().clear();
   }

   @Override
   public CompletionStage<Void> bulkWrite(int publisherCount, Publisher<SegmentedPublisher<MarshallableEntry<K, V>>> publisher) {
      return delegate().bulkWrite(publisherCount, publisher);
   }

   @Override
   public CompletionStage<Void> bulkDelete(int publisherCount, Publisher<SegmentedPublisher<Object>> publisher) {
      return delegate().bulkDelete(publisherCount, publisher);
   }

   @Override
   public CompletionStage<Long> size(IntSet segments) {
      return delegate().size(segments);
   }

   @Override
   public CompletionStage<Long> approximateSize(IntSet segments) {
      return delegate().approximateSize(segments);
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean includeValues) {
      return delegate().publishEntries(segments, filter, includeValues);
   }

   @Override
   public Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
      return delegate().publishKeys(segments, filter);
   }

   @Override
   public Publisher<MarshallableEntry<K, V>> purgeExpired() {
      return delegate().purgeExpired();
   }

   @Override
   public CompletionStage<Void> prepareWithModifications(Transaction transaction, BatchModification batchModification) {
      return delegate().prepareWithModifications(transaction, batchModification);
   }

   @Override
   public CompletionStage<Void> commit(Transaction transaction) {
      return delegate().commit(transaction);
   }

   @Override
   public CompletionStage<Void> rollback(Transaction transaction) {
      return delegate().rollback(transaction);
   }

   @Override
   public boolean ignoreCommandWithFlags(long commandFlags) {
      return delegate().ignoreCommandWithFlags(commandFlags);
   }
}
