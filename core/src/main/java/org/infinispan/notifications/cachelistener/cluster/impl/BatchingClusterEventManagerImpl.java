package org.infinispan.notifications.cachelistener.cluster.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.notifications.cachelistener.cluster.ClusterEvent;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventCallable;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventManager;
import org.infinispan.notifications.cachelistener.cluster.MultiClusterEventCallable;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.function.TriConsumer;

public class BatchingClusterEventManagerImpl<K, V> implements ClusterEventManager<K, V> {
   @Inject private ComponentRef<Cache<K, V>> cache;

   private ClusterExecutor clusterExecutor;

   private final ThreadLocal<EventContext<K, V>> localContext = new ThreadLocal<>();

   @Start
   public void start() {
      clusterExecutor = SecurityActions.getClusterExecutor(cache.wired()).singleNodeSubmission();
   }

   @Override
   public void addEvents(Address target, UUID identifier, Collection<ClusterEvent<K, V>> events, boolean sync) {
      EventContext<K, V> ctx = localContext.get();
      if (ctx == null) {
         ctx = new UnicastEventContext<>(cache.wired().getName());
         localContext.set(ctx);
      }
      ctx.addTargets(target, identifier, events, sync);
   }

   @Override
   public void sendEvents() {
      EventContext<K, V> ctx = localContext.get();
      if (ctx != null) {
         ctx.sendToTargets(clusterExecutor);
         localContext.remove();
      }
   }

   @Override
   public void dropEvents() {
      localContext.remove();
   }

   private interface EventContext<K, V> {
      void addTargets(Address address, UUID identifier, Collection<ClusterEvent<K, V>> events, boolean sync);

      void sendToTargets(ClusterExecutor executor);
   }

   protected static class UnicastEventContext<K, V> implements EventContext<K, V> {
      protected final String cacheName;
      protected final Map<Address, TargetEvents<K, V>> targets = new HashMap<>();

      public UnicastEventContext(String cacheName) {
         this.cacheName = cacheName;
      }

      @Override
      public void addTargets(Address address, UUID identifier, Collection<ClusterEvent<K, V>> events, boolean sync) {
         TargetEvents<K, V> targetEvents = targets.get(address);
         if (targetEvents == null) {
            targetEvents = new TargetEvents<>();
            targets.put(address, targetEvents);
         }

         Map<UUID, Collection<ClusterEvent<K, V>>> listenerEvents = targetEvents.events;
         // This shouldn't be set before, so do put instead of doing get then put
         Collection<ClusterEvent<K, V>> prevEvents = listenerEvents.put(identifier, events);
         if (prevEvents != null) {
            // If we have multiple events to the same node for the same uuid condense them.  This shouldn't really happen...
            events.addAll(prevEvents);
         }
         if (sync) {
            targetEvents.sync = true;
         }
      }

      @Override
      public void sendToTargets(ClusterExecutor executor) {
         List<CompletableFuture<Void>> futures = new ArrayList<>();
         TriConsumer<Address, Void, Throwable> triConsumer = (a, v, t) -> {
           if (t != null) {
              throw new CacheException(t);
           }
         };
         for (Entry<Address, TargetEvents<K, V>> entry : targets.entrySet()) {
            TargetEvents<K, V> value = entry.getValue();
            if (value.events.size() > 1) {
               CompletableFuture<Void> future = executor.filterTargets(Collections.singleton(entry.getKey()))
                     .submitConsumer(new MultiClusterEventCallable<>(cacheName, value.events), triConsumer);
               if (value.sync) {
                  futures.add(future);
               }
            } else if (value.events.size() == 1) {
               Entry<UUID, Collection<ClusterEvent<K, V>>> entryValue = value.events.entrySet().iterator().next();
               CompletableFuture<Void> future = executor.filterTargets(Collections.singleton(entry.getKey()))
                     .submitConsumer(new ClusterEventCallable<>(cacheName, entryValue.getKey(), entryValue.getValue()), triConsumer);
               if (value.sync) {
                  futures.add(future);
               }
            }
         }

         try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
         }
         catch (InterruptedException e) {
            throw new CacheException("Interrupted while waiting for event notifications to complete.", e);
         } catch (ExecutionException e) {
            throw new CacheException("Exception encountered while replicating events", e.getCause());
         }
      }
   }

   private static class TargetEvents<K, V> {
      final Map<UUID, Collection<ClusterEvent<K, V>>> events = new HashMap<>();
      boolean sync = false;
   }
}
