package org.infinispan.notifications.cachelistener;

import java.lang.invoke.MethodHandles;
import java.util.PrimitiveIterator;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.impl.ListenerInvocation;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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

   private final ToIntFunction<Object> intFunction;
   protected final InternalEntryFactory entryFactory;

   private PrimitiveIterator.OfInt justCompletedSegments = null;

   public DistributedQueueingSegmentListener(InternalEntryFactory entryFactory, int numSegments, ToIntFunction<Object> intFunction) {
      this.entryFactory = entryFactory;
      this.intFunction = intFunction;
      // we assume the # of segments won't change between different consistent hashes
      this.queues = new AtomicReferenceArray<>(numSegments);
      for (int i = 0; i < queues.length(); ++i) {
         Queue<KeyValuePair<CacheEntryEvent<K, V>, ListenerInvocation<Event<K, V>>>> queue = new ConcurrentLinkedQueue<>();
         queues.set(i, queue);
      }
   }

   @Override
   public boolean handleEvent(EventWrapper<K, V, CacheEntryEvent<K, V>> wrapped, ListenerInvocation<Event<K, V>> invocation) {
      K key = wrapped.getKey();
      // If we already completed, don't enqueue
      boolean enqueued = !completed.get();
      CacheEntryEvent<K, V> event = wrapped.getEvent();
      CacheEntry<K, V> cacheEntry = entryFactory.create(event.getKey(), event.getValue(), event.getMetadata());
      if (enqueued && !addEvent(key, cacheEntry.getValue() != null ? cacheEntry : REMOVED)) {
         // If it wasn't added it means we haven't processed this value yet, so add it to the queue for this segment
         int segment = intFunction.applyAsInt(key);
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
      AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      // Complete any segments that are still open - means we just didn't receive the completion yet
      // Iterator is guaranteed to not complete until all segments are complete.
      for (int i = 0; i < queues.length(); ++i) {
         if (queues.get(i) != null) {
            CompletionStage<Void> segmentStage = completeSegment(i);
            if (segmentStage != null) {
               aggregateCompletionStage.dependsOn(segmentStage);
            }
         }
      }
      completed.set(true);
      notifiedKeys.clear();
      return aggregateCompletionStage.freeze();
   }

   @Override
   public CompletionStage<Void> notifiedKey(K key) {
      // This relies on the fact that notifiedKey is immediately called after the entry has finished being iterated on
      PrimitiveIterator.OfInt iter = justCompletedSegments;
      AggregateCompletionStage<Void> aggregateCompletionStage = null;
      if (iter != null) {
         while (iter.hasNext()) {
            CompletionStage<Void> segmentStage = completeSegment(iter.nextInt());
            if (segmentStage != null) {
               if (aggregateCompletionStage == null) {
                  aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
               }
               aggregateCompletionStage.dependsOn(segmentStage);
            }
         }
      }
      justCompletedSegments = null;
      return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : CompletableFutures.completedNull();
   }

   private CompletionStage<Void> completeSegment(int segment) {
      Queue<KeyValuePair<CacheEntryEvent<K, V>, ListenerInvocation<Event<K, V>>>> queue = queues.getAndSet(segment, null);
      AggregateCompletionStage<Void> aggregateCompletionStage = null;
      if (queue != null) {
         if (trace) {
            log.tracef("Completed segment %s", segment);
         }
         if (!queue.isEmpty()) {
            aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
            for (KeyValuePair<CacheEntryEvent<K, V>, ListenerInvocation<Event<K, V>>> event : queue) {
               // The InitialTransferInvocation already did the converter if needed
               aggregateCompletionStage.dependsOn(event.getValue().invoke(event.getKey()));
            }
         }
      }
      return aggregateCompletionStage != null ? aggregateCompletionStage.freeze() : null;
   }

   @Override
   public void accept(Supplier<PrimitiveIterator.OfInt> segments) {
      justCompletedSegments = segments.get();
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
