package org.infinispan.notifications.cachelistener;

import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.impl.ListenerInvocation;
import org.infinispan.util.KeyValuePair;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

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
   protected final Queue<KeyValuePair<Event<K, V>, ListenerInvocation<Event<K, V>>>> queue =
         new ConcurrentLinkedQueue<>();
   protected final InternalEntryFactory entryFactory;

   QueueingAllSegmentListener(InternalEntryFactory entryFactory) {
      this.entryFactory = entryFactory;
   }

   @Override
   public boolean handleEvent(Event<K, V> event, ListenerInvocation<Event<K, V>> invocation) {
      boolean queued = !completed.get();
      if (queued) {
         boolean continueQueueing = true;
         if (event instanceof CacheEntryEvent) {
            CacheEntryEvent<K, V> cacheEvent = (CacheEntryEvent<K, V>) event;
            CacheEntry<K, V> cacheEntry = entryFactory.create(cacheEvent.getKey(), cacheEvent.getValue(),
                                                              cacheEvent.getMetadata());
            if (addEvent(cacheEntry.getKey(), cacheEntry.getValue() != null ? cacheEntry : REMOVED)) {
               continueQueueing = false;
            }
         }
         if (continueQueueing) {
            KeyValuePair<Event<K, V>, ListenerInvocation<Event<K, V>>> eventPair =
                  new KeyValuePair<Event<K, V>, ListenerInvocation<Event<K, V>>>(event, invocation);
            queue.add(eventPair);

            // If it completed since we last added and ours is in the queue, we have to run the event
            if (completed.get() && queue.remove(eventPair)) {
               invocation.invoke(event);
            }
         }
      }
      return queued;
   }

   @Override
   public void transferComplete() {
      Iterator<KeyValuePair<Event<K, V>, ListenerInvocation<Event<K, V>>>> iterator = queue.iterator();
      while (iterator.hasNext()) {
         KeyValuePair<Event<K, V>, ListenerInvocation<Event<K, V>>> eventPair = iterator.next();
         eventPair.getValue().invoke(eventPair.getKey());
         iterator.remove();
      }
      completed.set(true);
   }
}
