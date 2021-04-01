package org.infinispan.notifications.cachelistener.cluster.impl;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
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
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

@Scope(Scopes.NAMED_CACHE)
public class BatchingClusterEventManagerImpl<K, V> implements ClusterEventManager<K, V> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @Inject EmbeddedCacheManager cacheManager;
   @Inject Configuration configuration;
   @Inject RpcManager rpcManager;
   @Inject ComponentRef<CommandsFactory> commandsFactory;

   private long timeout;

   private final Map<Object, EventContext<K, V>> eventContextMap = new ConcurrentHashMap<>();

   @Start
   public void start() {
      timeout = configuration.clustering().remoteTimeout();
      configuration.clustering().attributes().attribute(ClusteringConfiguration.REMOTE_TIMEOUT)
                   .addListener((a, ignored) -> {
                      timeout = a.get();
                   });
   }

   @Override
   public void addEvents(Object batchIdentifier, Address target, UUID identifier, Collection<ClusterEvent<K, V>> events, boolean sync) {
      eventContextMap.compute(batchIdentifier, (ignore, eventContext) -> {
         if (eventContext == null) {
            if (log.isTraceEnabled()) {
               log.tracef("Created new unicast event context for identifier %s", batchIdentifier);
            }
            eventContext = new UnicastEventContext<>();
         }
         if (log.isTraceEnabled()) {
            log.tracef("Adding new events %s for identifier %s", events, batchIdentifier);
         }
         eventContext.addTargets(target, identifier, events, sync);
         return eventContext;
      });
   }

   @Override
   public CompletionStage<Void> sendEvents(Object batchIdentifier) {
      EventContext<K, V> ctx = eventContextMap.remove(batchIdentifier);
      if (ctx != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Sending events for identifier %s", batchIdentifier);
         }
         return ctx.sendToTargets();
      } else if (log.isTraceEnabled()) {
         log.tracef("No events to send for identifier %s", batchIdentifier);
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public void dropEvents(Object batchIdentifier) {
      EventContext<K, V> ctx = eventContextMap.remove(batchIdentifier);
      if (log.isTraceEnabled()) {
         if (ctx != null) {
            log.tracef("Dropping events for identifier %s", batchIdentifier);
         } else {
            log.tracef("No events to drop for identifier %s", batchIdentifier);
         }
      }
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
