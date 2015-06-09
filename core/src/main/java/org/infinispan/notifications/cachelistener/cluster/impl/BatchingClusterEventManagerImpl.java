package org.infinispan.notifications.cachelistener.cluster.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.distexec.DistributedExecutionCompletionService;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.factories.annotations.Start;
import org.infinispan.notifications.cachelistener.cluster.ClusterEvent;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventCallable;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventManager;
import org.infinispan.notifications.cachelistener.cluster.MultiClusterEventCallable;
import org.infinispan.remoting.transport.Address;

public class BatchingClusterEventManagerImpl<K, V> implements ClusterEventManager<K, V>{
   private final Cache<K, V> cache;
   
   private DistributedExecutorService distExecService;
   
   private final ThreadLocal<EventContext<K, V>> localContext = new ThreadLocal<>();
   
   public BatchingClusterEventManagerImpl(Cache<K, V> cache) {
      this.cache = cache;
   }
   
   @Start
   public void start() {
      distExecService = SecurityActions.getDefaultExecutorService(cache);
   }
   
   @Override
   public void addEvents(Address target, UUID identifier, Collection<ClusterEvent<K, V>> events, boolean sync) {
      EventContext<K, V> ctx = localContext.get();
      if (ctx == null) {
         ctx = new UnicastEventContext<K, V>();
         localContext.set(ctx);
      }
      ctx.addTargets(target, identifier, events, sync);
   }

   @Override
   public void sendEvents() {
      EventContext<K, V> ctx = localContext.get();
      if (ctx != null) {
         ctx.sendToTargets(distExecService);
         localContext.remove();
      }
   }
   
   @Override
   public void dropEvents() {
      localContext.remove();
   }
   
   private static interface EventContext<K, V> {
      public void addTargets(Address address, UUID identifier, Collection<ClusterEvent<K, V>> events, boolean sync);
      
      public void sendToTargets(DistributedExecutorService service);
   }
   
   protected static class UnicastEventContext<K, V> implements EventContext<K, V> {
      protected final Map<Address, TargetEvents<K, V>> targets = new HashMap<>();

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
      public void sendToTargets(DistributedExecutorService service) {
         DistributedExecutionCompletionService<Void> completion = new DistributedExecutionCompletionService<Void>(service);
         int syncCount = 0;
         for (Entry<Address, TargetEvents<K, V>> entry : targets.entrySet()) {
            TargetEvents<K, V> value = entry.getValue();
            if (value.events.size() > 1) {
               if (value.sync) {
                  completion.submit(entry.getKey(), new MultiClusterEventCallable<>(value.events));
                  syncCount++;
               } else {
                  service.submit(entry.getKey(), new MultiClusterEventCallable<>(value.events));
               }
            } else if (value.events.size() == 1) {
               Entry<UUID, Collection<ClusterEvent<K, V>>> entryValue = value.events.entrySet().iterator().next();
               if (value.sync) {
                  completion.submit(entry.getKey(), new ClusterEventCallable<K, V>(entryValue.getKey(), entryValue.getValue()));
                  syncCount++;
               } else {
                  service.submit(entry.getKey(), new ClusterEventCallable<K, V>(entryValue.getKey(), entryValue.getValue()));
               }
            }
         }
         
         try {
            for (int i = 0; i < syncCount; ++i) {
               completion.take();
            }
         }
         catch (InterruptedException e) {
            throw new CacheException("Interrupted while waiting for event notifications to complete.", e);
         }
      }
   }
   
   private static class TargetEvents<K, V> {
      final Map<UUID, Collection<ClusterEvent<K, V>>> events = new HashMap<>();
      boolean sync = false;
   }
}
