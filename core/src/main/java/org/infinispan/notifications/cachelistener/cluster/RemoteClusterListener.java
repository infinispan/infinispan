package org.infinispan.notifications.cachelistener.cluster;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.annotation.TransactionCompleted;
import org.infinispan.notifications.cachelistener.annotation.TransactionRegistered;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent;
import org.infinispan.notifications.cachelistener.event.TransactionRegisteredEvent;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * A listener that installed locally on each node when a cluster listener is installed on a given node.
 *
 * @author wburns
 * @since 7.0
 */
@ThreadSafe
@Listener(primaryOnly = true)
public class RemoteClusterListener {
   private static final Log log = LogFactory.getLog(RemoteClusterListener.class);

   private final UUID id;
   private final Address origin;
   private final DistributedExecutorService distExecService;
   private final CacheNotifier cacheNotifier;
   private final CacheManagerNotifier cacheManagerNotifier;

   private final ConcurrentMap<GlobalTransaction, Queue<CacheEntryEvent>> transactionChanges =
         CollectionFactory.makeConcurrentMap();

   public RemoteClusterListener(UUID id, Address origin, DistributedExecutorService distExecService, CacheNotifier cacheNotifier,
                                CacheManagerNotifier cacheManagerNotifier) {
      this.id = id;
      this.origin = origin;
      this.distExecService = distExecService;
      this.cacheNotifier = cacheNotifier;
      this.cacheManagerNotifier = cacheManagerNotifier;
   }

   public UUID getId() {
      return id;
   }

   public Address getOwnerAddress() {
      return origin;
   }

   @ViewChanged
   public void viewChange(ViewChangedEvent event) {
      if (!event.getNewMembers().contains(origin)) {
         if (log.isTraceEnabled()) {
            log.tracef("Origin %s storing cluster listener is gone, removing local listener", origin);
         }
         removeListener();
      }
   }

   public void removeListener() {
      cacheNotifier.removeListener(this);
      cacheManagerNotifier.removeListener(this);
   }

   @CacheEntryCreated
   @CacheEntryModified
   @CacheEntryRemoved
   public void handleClusterEvents(CacheEntryEvent event) throws Exception {
      // We only submit the final event
      if (!event.isPre()) {
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
            if (log.isTraceEnabled()) {
               log.tracef("Submitting Event %s to cluster listener to %s", event, origin);
            }
            distExecService.submit(origin, new ClusterEventCallable(id, ClusterEvent.fromEvent(event))).get();
         }
      }
   }

   @TransactionCompleted
   public void transactionCompleted(TransactionCompletedEvent event) throws Exception {
      Queue<CacheEntryEvent> events = transactionChanges.remove(event.getGlobalTransaction());
      if (event.isTransactionSuccessful() && events != null) {
         List<ClusterEvent> eventsToSend = new ArrayList<>(events.size());
         for (CacheEntryEvent cacheEvent : events) {
            eventsToSend.add(ClusterEvent.fromEvent(cacheEvent));
            // Send event back to origin who has the cluster listener
            if (log.isTraceEnabled()) {
               log.tracef("Submitting Event(s) %s to cluster listener to %s", eventsToSend, origin);
            }
         }
         // Force the execution to wait until completed
         distExecService.submit(origin, new ClusterEventCallable(id, eventsToSend)).get();
      }
   }
}
