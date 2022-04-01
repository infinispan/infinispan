package org.infinispan.notifications.cachelistener;

import java.lang.invoke.MethodHandles;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.impl.ListenerInvocation;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;

/**
 * This handler is to be used with a clustered distributed cache.  This handler does special optimizations to
 * alllow for queueing to occur per segment.  This way we don't retain all new events in memory unlike
 * {@link QueueingAllSegmentListener} until the iteration is complete.
 *
 * @author wburns
 * @since 7.0
 */
class DistributedQueueingSegmentListener<K, V> extends BaseQueueingSegmentListener<K, V, CacheEntryEvent<K, V>> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private final AtomicReferenceArray<Queue<KeyValuePair<CacheEntryEvent<K, V>, ListenerInvocation<Event<K, V>>>>> queues;

   protected final InternalEntryFactory entryFactory;

   public DistributedQueueingSegmentListener(InternalEntryFactory entryFactory, int numSegments, KeyPartitioner keyPartitioner) {
      super(numSegments, keyPartitioner);
      this.entryFactory = entryFactory;
      // we assume the # of segments won't change between different consistent hashes
      this.queues = new AtomicReferenceArray<>(numSegments);
      for (int i = 0; i < queues.length(); ++i) {
         queues.set(i, new ConcurrentLinkedQueue<>());
      }
   }

   @Override
   public boolean handleEvent(EventWrapper<K, V, CacheEntryEvent<K, V>> wrapped, ListenerInvocation<Event<K, V>> invocation) {
      // If we already completed, don't even attempt to enqueue
      if (completed.get()) {
         return false;
      }
      K key = wrapped.getKey();
      CacheEntryEvent<K, V> event = wrapped.getEvent();
      int segment = segmentFromEventWrapper(wrapped);
      CacheEntry<K, V> cacheEntry = entryFactory.create(event.getKey(), event.getValue(), event.getMetadata());
      boolean enqueued = true;
      if (!addEvent(key, segment, cacheEntry.getValue() != null ? cacheEntry : REMOVED)) {
         // If it wasn't added it means we haven't processed this value yet, so add it to the queue for this segment
         Queue<KeyValuePair<CacheEntryEvent<K, V>, ListenerInvocation<Event<K, V>>>> queue;
         // If the queue is not null, try to see if we can add to it
         if ((queue = queues.get(segment)) != null) {
            KeyValuePair<CacheEntryEvent<K, V>, ListenerInvocation<Event<K, V>>> eventPair =
                  new KeyValuePair<>(event, invocation);
            queue.add(eventPair);
            // If the queue was removed, that means we had a concurrent completion, so we need to verify if we
            // have to run the event manually
            if (queues.get(segment) == null) {
               if (queue.remove(eventPair)) {
                  enqueued = false;
               }
            }
         } else {
            // if the queue is already null that means it was transferred so just raise the notification
            enqueued = false;
         }
      }
      return enqueued;
   }

   @Override
   public CompletionStage<Void> transferComplete() {
      completed.set(true);
      for (int i = 0; i < notifiedKeys.length(); ++i) {
         assert notifiedKeys.get(i) == null;
         assert queues.get(i) == null;
      }
      return CompletableFutures.completedNull();
   }

   @Override
   Flowable<CacheEntry<K, V>> segmentComplete(int segment) {
      return super.segmentComplete(segment)
            .concatWith(Completable.defer(() -> Completable.fromCompletionStage(completeSegment(segment))));
   }

   private CompletionStage<Void> completeSegment(int segment) {
      Queue<KeyValuePair<CacheEntryEvent<K, V>, ListenerInvocation<Event<K, V>>>> queue = queues.getAndSet(segment, null);
      AggregateCompletionStage<Void> aggregateCompletionStage = null;
      if (queue != null) {
         if (!queue.isEmpty()) {
            aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
            for (KeyValuePair<CacheEntryEvent<K, V>, ListenerInvocation<Event<K, V>>> event : queue) {
               // The InitialTransferInvocation already did the converter if needed
               aggregateCompletionStage.dependsOn(event.getValue().invoke(event.getKey()));
            }
         }
      }
      return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : CompletableFutures.completedNull();
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
