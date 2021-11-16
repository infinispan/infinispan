package org.infinispan.notifications.cachelistener;

import java.util.concurrent.CompletionStage;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.impl.ListenerInvocation;
import org.infinispan.reactive.publisher.impl.SegmentPublisherSupplier;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.functions.Function;

/**
 * This interface describes methods needed for a segment listener that is used when iterating over the current
 * events and be able to queue them properly
 *
 * @author wburns
 * @since 7.0
 */
public interface QueueingSegmentListener<K, V, E extends Event<K, V>> extends Function<SegmentPublisherSupplier.Notification<CacheEntry<K, V>>, Publisher<CacheEntry<K, V>>> {
   // This is to be used as a placeholder when a value has been iterated and now is being processed by the caller
   // This is considered to be the completed state for the key and should never change from this
   static final Object NOTIFIED = new Object();
   // This is to be used as a placeholder for a removed value.  This is needed so that we know
   // a value is removed.  The caller will get this back when processing a key and should then ignore
   // or do it's own special processing for removed values
   static final Object REMOVED = new Object();

   /**
    * This should be invoked on a notification before actually processing the data.
    * Note this method modifies the underlying listener state.
    * It will return a Publisher with the entries that need to be notified or an empty Publisher if none
    *
    * @param cacheEntryNotification The notification being processed
    * @return a Publisher that returns all the CacheEntries that need to be notified
    */
   @Override
   Publisher<CacheEntry<K, V>> apply(SegmentPublisherSupplier.Notification<CacheEntry<K, V>> cacheEntryNotification) throws Throwable;

   /**
    * This should be called by any listener when an event is generated to possibly queue it.  If it is not
    * queued, then the caller should take appropriate action such as manually firing the invocation.
    * @param wrapper The event that was just raised
    * @param invocation The invocation the event would be fired on
    * @return Whether or not it was queued.  If it wasn't queued the invocation should be fired manually
    */
   public boolean handleEvent(EventWrapper<K, V, E> wrapper, ListenerInvocation<Event<K, V>> invocation);

   /**
    * This is needed to tell the handler when the complete iteration is done.  Depending on the implementation
    * this could also fire all queued events that are remaining.
    */
   public CompletionStage<Void> transferComplete();
}
