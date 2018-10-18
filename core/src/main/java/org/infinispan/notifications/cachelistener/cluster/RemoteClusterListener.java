package org.infinispan.notifications.cachelistener.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.annotation.TransactionCompleted;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.ThreadSafe;

/**
 * A listener that installed locally on each node when a cluster listener is installed on a given node.
 *
 * @author wburns
 * @since 7.0
 */
@ThreadSafe
@Listener(primaryOnly = true, observation = Listener.Observation.POST)
public class RemoteClusterListener {
   private static final Log log = LogFactory.getLog(RemoteClusterListener.class);
   private static final boolean trace = log.isTraceEnabled();

   private final UUID id;
   private final Address origin;
   private final CacheNotifier cacheNotifier;
   private final CacheManagerNotifier cacheManagerNotifier;
   private final ClusterEventManager eventManager;
   private final boolean sync;

   private final ConcurrentMap<GlobalTransaction, Queue<CacheEntryEvent>> transactionChanges =
         new ConcurrentHashMap<>();

   public RemoteClusterListener(UUID id, Address origin, CacheNotifier cacheNotifier,
                                CacheManagerNotifier cacheManagerNotifier, ClusterEventManager eventManager, boolean sync) {
      this.id = id;
      this.origin = origin;
      this.cacheNotifier = cacheNotifier;
      this.cacheManagerNotifier = cacheManagerNotifier;
      this.eventManager = eventManager;
      this.sync = sync;
   }

   public UUID getId() {
      return id;
   }

   public Address getOwnerAddress() {
      return origin;
   }

   @ViewChanged
   public CompletionStage<Void> viewChange(ViewChangedEvent event) {
      if (!event.getNewMembers().contains(origin)) {
         if (trace) {
            log.tracef("Origin %s storing cluster listener is gone, removing local listener", origin);
         }
         return removeListener();
      }
      return CompletableFutures.completedNull();
   }

   public CompletionStage<Void> removeListener() {
      return CompletionStages.allOf(cacheNotifier.removeListenerAsync(this),
            cacheManagerNotifier.removeListenerAsync(this));
   }

   @CacheEntryCreated
   @CacheEntryModified
   @CacheEntryRemoved
   @CacheEntryExpired
   public CompletionStage<Void> handleClusterEvents(CacheEntryEvent event) throws Exception {
      GlobalTransaction transaction = event.getGlobalTransaction();
      if (transaction != null) {
         // If we are in a transaction, queue up those events so we can send them as 1 batch.
         Queue<CacheEntryEvent> events = transactionChanges.get(transaction);
         if (events == null) {
            events = new ConcurrentLinkedQueue<>();
            Queue<CacheEntryEvent> otherQueue = transactionChanges.putIfAbsent(transaction, events);
            if (otherQueue != null) {
               events = otherQueue;
            }
         }
         events.add(event);
      }  else {
         // Send event back to origin who has the cluster listener
         if (trace) {
            log.tracef("Passing Event to manager %s to send to %s", event, origin);
         }
         eventManager.addEvents(origin, id, Collections.singleton(ClusterEvent.fromEvent(event)), sync);
      }
      return CompletableFutures.completedNull();
   }

   @TransactionCompleted
   public CompletionStage<Void> transactionCompleted(TransactionCompletedEvent event) throws Exception {
      Queue<CacheEntryEvent> events = transactionChanges.remove(event.getGlobalTransaction());
      if (event.isTransactionSuccessful() && events != null) {
         List<ClusterEvent> eventsToSend = new ArrayList<>(events.size());
         for (CacheEntryEvent cacheEvent : events) {
            eventsToSend.add(ClusterEvent.fromEvent(cacheEvent));
            // Send event back to origin who has the cluster listener
            if (trace) {
               log.tracef("Passing Event(s) to manager %s to send to %s", eventsToSend, origin);
            }
         }
         eventManager.addEvents(origin, id, eventsToSend, sync);
      }
      return CompletableFutures.completedNull();
   }
}
