package org.infinispan.persistence.support;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.infinispan.commons.util.IntSet;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.util.concurrent.CompletionStages;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

public interface WaitNonBlockingStore<K, V> extends NonBlockingStore<K, V> {
   KeyPartitioner getKeyPartitioner();

   default boolean delete(Object key) {
      int segment = getKeyPartitioner().getSegment(key);
      return join(delete(segment, key));
   }

   default boolean contains(Object key) {
      int segment = getKeyPartitioner().getSegment(key);
      return join(containsKey(segment, key));
   }

   default MarshallableEntry<K, V> loadEntry(Object key) {
      int segment = getKeyPartitioner().getSegment(key);
      return join(load(segment, key));
   }

   default void write(MarshallableEntry<K, V> entry) {
      int segment = getKeyPartitioner().getSegment(entry.getKey());
      join(write(segment, entry));
   }

   default void bulkUpdate(int numSegments, Publisher<MarshallableEntry<? extends K, ? extends V>> publisher) {
      Publisher<MarshallableEntry<K, V>> castPublisher = (Publisher) publisher;
      join(bulkWrite(numSegments, Flowable.fromPublisher(castPublisher)
            .groupBy(me -> getKeyPartitioner().getSegment(me.getKey()))
            .map(SegmentPublisherWrapper::new)));
   }

   default void deleteBatch(int numSegments, Iterable<Object> keys) {
      join(bulkDelete(numSegments, Flowable.fromIterable(keys)
            .groupBy(getKeyPartitioner()::getSegment)
            .map(SegmentPublisherWrapper::new)));
   }

   default boolean checkAvailable() {
      return join(isAvailable());
   }

   default long sizeWait(IntSet segments) {
      return join(size(segments));
   }

   default void clearAndWait() {
      join(clear());
   }

   default void stopAndWait() {
      join(stop());
   }

   default List<K> publishKeysWait(IntSet segments, Predicate<? super K> filter) {
      return join(Flowable.fromPublisher(publishKeys(segments, filter))
            .collect(Collectors.toList())
            .toCompletionStage());
   }

   default List<MarshallableEntry<K, V>> purge() {
      return join(Flowable.fromPublisher(purgeExpired())
            .collect(Collectors.toList())
            .toCompletionStage());
   }

   // This method is here solely for byte code augmentation with blockhound
   default <V> V join(CompletionStage<V> stage) {
      return CompletionStages.join(stage);
   }
}
