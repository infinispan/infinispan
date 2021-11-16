package org.infinispan.notifications.cachelistener;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.reactive.RxJavaInterop;
import org.infinispan.reactive.publisher.impl.SegmentPublisherSupplier;
import org.infinispan.util.logging.Log;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

/**
 * This is the base class for use when listening to segment completions when doing initial event
 * retrieval.  This will handle keeping track of concurrent key updates as well as iteration by calling
 * appropriate methods at the given time.
 * <p>
 * This base class provides a working set for tracking of entries as they are iterated on, assuming
 * the {@link QueueingSegmentListener#apply(SegmentPublisherSupplier.Notification)}
 * method is invoked for each event serially (includes both segment and entries).
 *
 * @author wburns
 * @since 7.0
 */
abstract class BaseQueueingSegmentListener<K, V, E extends Event<K, V>> implements QueueingSegmentListener<K, V, E> {
   protected final AtomicBoolean completed = new AtomicBoolean(false);
   protected final AtomicReferenceArray<ConcurrentMap<K, Object>> notifiedKeys;
   protected final KeyPartitioner keyPartitioner;

   protected BaseQueueingSegmentListener(int numSegments, KeyPartitioner keyPartitioner) {
      this.notifiedKeys = new AtomicReferenceArray<>(numSegments);
      this.keyPartitioner = keyPartitioner;
      for (int i = 0; i < numSegments; ++i) {
         notifiedKeys.set(i, new ConcurrentHashMap<>());
      }
   }

   int segmentFromEventWrapper(EventWrapper<K, V, E> eventWrapper) {
      return SegmentSpecificCommand.extractSegment(eventWrapper.getCommand(), eventWrapper.getKey(), keyPartitioner);
   }

   @Override
   public Publisher<CacheEntry<K, V>> apply(SegmentPublisherSupplier.Notification<CacheEntry<K, V>> cacheEntryNotification) throws Throwable {
      if (cacheEntryNotification.isSegmentComplete()) {
         return segmentComplete(cacheEntryNotification.completedSegment());
      }

      int segment = cacheEntryNotification.valueSegment();

      CacheEntry<K, V> cacheEntry = cacheEntryNotification.value();
      K key = cacheEntry.getKey();
      // By putting the NOTIFIED value it has signaled that any more updates for this key have to be enqueued instead
      // of taking the last one
      Object value = notifiedKeys.get(segment).put(key, NOTIFIED);
      if (value == null)
         return Flowable.just(cacheEntry);

      if (getLog().isTraceEnabled()) {
         getLog().tracef("Processing key %s as a concurrent update occurred with value %s", key, value);
      }
      return value != QueueingSegmentListener.REMOVED ? Flowable.just(((CacheEntry<K, V>) value)) : Flowable.empty();
   }


   Flowable<CacheEntry<K, V>> segmentComplete(int segment) {
      ConcurrentMap<K, Object> map = notifiedKeys.get(segment);
      // Ensure `addEvent` below knows if the value was added or not
      synchronized (map) {
         notifiedKeys.set(segment, null);
      }
      return Flowable.fromIterable(map.entrySet())
            // We only process entries we can remove ourselves to guarantee consistency with atomic updates
            // Normally we would use iterator, but remove for iterator doesn't notify us if it actually removed the entry
            .filter(e -> map.remove(e.getKey()) != null)
            .map(RxJavaInterop.entryToValueFunction())
            .filter(v -> v != NOTIFIED && v != REMOVED)
            .map(v -> (CacheEntry<K, V>) v);
   }

   protected boolean addEvent(K key, int segment, Object value) {
      ConcurrentMap<K, Object> map = notifiedKeys.get(segment);
      if (map == null) {
         return false;
      }

      synchronized (map) {
         // Need to double check inside synchronized just in case concurrent segmentComplete occurred
         if (notifiedKeys.get(segment) == map) {
            return map.compute(key, (k, v) -> v == NOTIFIED ? v : value) == value;
         }
      }
      return false;
   }

   protected abstract Log getLog();
}
