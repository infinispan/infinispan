package org.infinispan.notifications.cachelistener;

import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.impl.ListenerInvocation;
import org.infinispan.util.KeyValuePair;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * This handler is to be used with a clustered distributed cache.  This handler does special optimizations to
 * alllow for queueing to occur per segment.  This way we don't retain all new events in memory unlike
 * {@link QueueingAllSegmentListener} until the iteration is complete.
 *
 * @author wburns
 * @since 7.0
 */
class DistributedQueueingSegmentListener<K, V> extends BaseQueueingSegmentListener<K, V, CacheEntryEvent<K, V>> {
   private final AtomicReferenceArray<Queue<KeyValuePair<CacheEntryEvent<K, V>, ListenerInvocation<Event<K, V>>>>> queues;

   private final DistributionManager distributionManager;
   protected final InternalEntryFactory entryFactory;

   private int justCompletedSegment = -1;

   public DistributedQueueingSegmentListener(InternalEntryFactory entryFactory, DistributionManager distributionManager) {
      this.entryFactory = entryFactory;
      this.distributionManager = distributionManager;
      // we assume the # of segments won't change between different consistent hashes
      this.queues = new AtomicReferenceArray(distributionManager.getReadConsistentHash().getNumSegments());
      for (int i = 0; i < queues.length(); ++i) {
         Queue<KeyValuePair<CacheEntryEvent<K, V>, ListenerInvocation<Event<K, V>>>> queue = new ConcurrentLinkedQueue<>();
         queues.set(i, queue);
      }
   }

   @Override
   public boolean handleEvent(CacheEntryEvent<K, V> event, ListenerInvocation<Event<K, V>> invocation) {
      K key = event.getKey();
      // If we already completed, don't enqueue
      boolean enqueued = !completed.get();
      CacheEntry<K, V> cacheEntry = entryFactory.create(event.getKey(), event.getValue(), event.getMetadata());
      if (enqueued && !addEvent(key, cacheEntry.getValue() != null ? cacheEntry : REMOVED)) {
         // If it wasn't added it means we haven't processed this value yet, so add it to the queue for this segment
         int segment = distributionManager.getReadConsistentHash().getSegment(key);
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
      completed.set(true);
      notifiedKeys.clear();
      for (int i = 0; i < queues.length(); ++i) {
         queues.set(i, null);
      }
   }

   public Object markKeyAsProcessing(K key) {
      // By putting the NOTIFIED value it has signaled that any more updates for this key have to be enqueud instead
      // of taking the last one
      return notifiedKeys.put(key, NOTIFIED);
   }

   @Override
   public void notifiedKey(K key) {
      // This relies on the fact that notifiedKey is immediately called after the entry has finished being iterated on
      if (justCompletedSegment != -1) {
         completeSegment(justCompletedSegment);
      }
      justCompletedSegment = -1;
   }

   private void completeSegment(int segment) {
      Queue<KeyValuePair<CacheEntryEvent<K, V>, ListenerInvocation<Event<K, V>>>> queue = queues.get(segment);
      if (queue != null) {
         for (KeyValuePair<CacheEntryEvent<K, V>, ListenerInvocation<Event<K, V>>> event : queue) {
            // The InitialTransferInvocation already did the converter if needed
            event.getValue().invoke(event.getKey());
         }
         queues.set(segment, null);
      }
   }

   public void segmentTransferred(int segment, boolean sentLastEntry) {
      if (queues.get(segment) != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Completed segment %s", segment);
         }
         if (sentLastEntry) {
            justCompletedSegment = segment;
         } else {
            completeSegment(segment);
         }
      }
   }
}
