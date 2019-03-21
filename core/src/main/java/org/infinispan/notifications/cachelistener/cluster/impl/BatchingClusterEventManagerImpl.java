package org.infinispan.notifications.cachelistener.cluster.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.cluster.ClusterEvent;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventManager;
import org.infinispan.notifications.cachelistener.cluster.MultiClusterEventCommand;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.AggregateCompletionStage;

public class BatchingClusterEventManagerImpl<K, V> implements ClusterEventManager<K, V> {
   @Inject private EmbeddedCacheManager cacheManager;
   @Inject private Configuration configuration;
   @Inject private RpcManager rpcManager;
   @Inject private ComponentRef<CommandsFactory> commandsFactory;

   private long timeout;

   private final ThreadLocal<EventContext<K, V>> localContext = new ThreadLocal<>();

   @Start
   public void start() {
      timeout = configuration.clustering().remoteTimeout();
   }

   @Override
   public void addEvents(Address target, UUID identifier, Collection<ClusterEvent<K, V>> events, boolean sync) {
      EventContext<K, V> ctx = localContext.get();
      if (ctx == null) {
         ctx = new UnicastEventContext<>();
         localContext.set(ctx);
      }
      ctx.addTargets(target, identifier, events, sync);
   }

   @Override
   public CompletionStage<Void> sendEvents() {
      EventContext<K, V> ctx = localContext.get();
      if (ctx != null) {
         localContext.remove();
         return ctx.sendToTargets();
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public void dropEvents() {
      localContext.remove();
   }

   private interface EventContext<K, V> {
      void addTargets(Address address, UUID identifier, Collection<ClusterEvent<K, V>> events, boolean sync);

      CompletionStage<Void> sendToTargets();
   }

   protected class UnicastEventContext<K, V> implements EventContext<K, V> {
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
      public CompletionStage<Void> sendToTargets() {
         AggregateCompletionStage<Void> aggregateCompletionStage = CompletionStages.aggregateCompletionStage();
         CommandsFactory factory = commandsFactory.running();
         for (Entry<Address, TargetEvents<K, V>> entry : targets.entrySet()) {
            TargetEvents<K, V> multiEvents = entry.getValue();
            MultiClusterEventCommand<K, V> callable = factory.buildMultiClusterEventCommand(multiEvents.events);
            CompletionStage<ValidResponse> stage = rpcManager.invokeCommand(entry.getKey(), callable, SingleResponseCollector.validOnly(),
                  new RpcOptions(DeliverOrder.NONE, timeout, TimeUnit.MILLISECONDS));
            if (multiEvents.sync) {
               aggregateCompletionStage.dependsOn(stage);
            }
         }
         return aggregateCompletionStage.freeze();
      }
   }

   private static class TargetEvents<K, V> {
      final Map<UUID, Collection<ClusterEvent<K, V>>> events = new HashMap<>();
      boolean sync = false;
   }
}
