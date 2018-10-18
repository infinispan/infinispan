package org.infinispan.notifications.cachelistener;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.impl.ListenerInvocation;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This handler is to be used when all the events must be queued until the iteration process is complete.
 * This is required for any local listener or non distributed caches.  The local is required since we
 * could have other events that are interrelated such as tx start/stop that all must be queued together in
 * the order they were provided.
 *
 * @author wburns
 * @since 7.0
 */
class QueueingAllSegmentListener<K, V> extends BaseQueueingSegmentListener<K, V, Event<K, V>> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   protected final Queue<KeyValuePair<Event<K, V>, ListenerInvocation<Event<K, V>>>> queue =
         new ConcurrentLinkedQueue<>();
   protected final InternalEntryFactory entryFactory;

   QueueingAllSegmentListener(InternalEntryFactory entryFactory) {
      this.entryFactory = entryFactory;
   }

   @Override
   public boolean handleEvent(EventWrapper<K, V, Event<K, V>> wrapper, ListenerInvocation<Event<K, V>> invocation) {
      boolean queued = !completed.get();
      if (queued) {
         boolean continueQueueing = true;
         Event<K, V> event = wrapper.getEvent();
         if (event instanceof CacheEntryEvent) {
            CacheEntryEvent<K, V> cacheEvent = (CacheEntryEvent<K, V>) event;
            CacheEntry<K, V> cacheEntry = entryFactory.create(cacheEvent.getKey(), cacheEvent.getValue(),
                                                              cacheEvent.getMetadata());
            if (addEvent(wrapper.getKey(), cacheEntry.getValue() != null ? cacheEntry : REMOVED)) {
               continueQueueing = false;
            }
         }
         if (continueQueueing) {
            KeyValuePair<Event<K, V>, ListenerInvocation<Event<K, V>>> eventPair =
                  new KeyValuePair<Event<K, V>, ListenerInvocation<Event<K, V>>>(event, invocation);
            queue.add(eventPair);

            // If it completed since we last added and ours is in the queue, we have to run the event - so say it wasn't
            // queued, so caller has to run it
            if (completed.get() && queue.remove(eventPair)) {
               return false;
            }
         }
      }
      return queued;
   }

   @Override
   public CompletionStage<Void> transferComplete() {
      AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
      Iterator<KeyValuePair<Event<K, V>, ListenerInvocation<Event<K, V>>>> iterator = queue.iterator();
      while (iterator.hasNext()) {
         KeyValuePair<Event<K, V>, ListenerInvocation<Event<K, V>>> eventPair = iterator.next();
         CompletionStage<Void> eventStage = eventPair.getValue().invoke(eventPair.getKey());
         if (eventStage != null) {
            aggregateCompletionStage.dependsOn(eventStage);
         }
         iterator.remove();
      }
      completed.set(true);
      return aggregateCompletionStage.freeze();
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
