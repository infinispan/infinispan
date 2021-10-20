package org.infinispan.notifications.cachelistener;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.impl.ListenerInvocation;
import org.infinispan.reactive.publisher.impl.SegmentCompletionPublisher;
import org.infinispan.util.concurrent.CompletableFutures;

import io.reactivex.rxjava3.functions.Function;

/**
 * This interface describes methods needed for a segment listener that is used when iterating over the current
 * events and be able to queue them properly
 *
 * @author wburns
 * @since 7.0
 */
public interface QueueingSegmentListener<K, V, E extends Event<K, V>> extends Function<SegmentCompletionPublisher.Notification<CacheEntry<K, V>>, Optional<? extends CacheEntry<K, V>>> {
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
    * If it is a value it will determine if the entry should notify the listener by returning the value or empty.
    * If the notification is a segment completion it will keep track of that and will always return
    * {@link java.util.Optional#empty} so only values can pass this filter.
    *
    * @param cacheEntryNotification The notification being processed
    * @return an Optional with a value if it is should be processed
    */
   @Override
   Optional<CacheEntry<K, V>> apply(SegmentCompletionPublisher.Notification<CacheEntry<K, V>> cacheEntryNotification) throws Throwable;

   /**
    * This method is to be called just before marking the transfer as complete and after all keys have been manually
    * processed.  This will return all the entries that were raised in an event but not manually marked.  This
    * is indicative of a CREATE event occurring but not seeing the value.
    *
    * @return
    */
   public Set<CacheEntry<K, V>> findCreatedEntries();

   /**
    * Invoked to determine if processing should be delayed or not. Will return
    * {@link CompletableFutures#completedNull()} if processing can continue immediately, otherwise should wait until this
    * is complete.
    * @return null if no notifications are required or a non null CompletionStage that when completed all notifications are done
    */
   public CompletionStage<Void> delayProcessing();

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
