package org.infinispan.notifications.cachelistener;

import java.lang.invoke.MethodHandles;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.impl.ListenerInvocation;
import org.infinispan.util.KeyValuePair;
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

   private Stream<Integer> justCompletedSegments = Stream.empty();

   private final Consumer<Integer> completeSegment = this::completeSegment;

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
   public void transferComplete() {
      // Complete any segments that are still open - means we just didn't receive the completion yet
      // Iterator is guaranteed to not complete until all segments are complete.
      for (int i = 0; i < queues.length(); ++i) {
         if (queues.get(i) != null) {
            completeSegment(i);
         }
      }
      completed.set(true);
      notifiedKeys.clear();
   }

   public Object markKeyAsProcessing(K key) {
      // By putting the NOTIFIED value it has signaled that any more updates for this key have to be enqueud instead
      // of taking the last one
      return notifiedKeys.put(key, NOTIFIED);
   }

   @Override
   public void notifiedKey(K key) {
      // This relies on the fact that notifiedKey is immediately called after the entry has finished being iterated on
      justCompletedSegments.forEach(completeSegment);
      justCompletedSegments = Stream.empty();
   }

   private void completeSegment(int segment) {
      Queue<KeyValuePair<CacheEntryEvent<K, V>, ListenerInvocation<Event<K, V>>>> queue = queues.getAndSet(segment, null);
      if (queue != null) {
         if (trace) {
            log.tracef("Completed segment %s", segment);
         }
         for (KeyValuePair<CacheEntryEvent<K, V>, ListenerInvocation<Event<K, V>>> event : queue) {
            // The InitialTransferInvocation already did the converter if needed
            event.getValue().invoke(event.getKey());
         }
      }
   }

   public void segmentCompleted(Set<Integer> segments) {
      justCompletedSegments = segments.stream().filter(s -> queues.get(s) != null);
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
