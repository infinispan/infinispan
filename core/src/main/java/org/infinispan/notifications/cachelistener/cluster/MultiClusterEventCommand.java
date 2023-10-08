package org.infinispan.notifications.cachelistener.cluster;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This command is used to send cluster events for multiple listeners on the same node
 *
 * @author wburns
 * @since 10.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTI_CLUSTER_EVENT_COMMAND)
public class MultiClusterEventCommand<K, V> extends BaseRpcCommand {

   public static final int COMMAND_ID = 19;

   private static final Log log = LogFactory.getLog(MultiClusterEventCommand.class);

   private final Map<UUID, Collection<ClusterEvent<K, V>>> multiEvents;

   public MultiClusterEventCommand(ByteString cacheName, Map<UUID, Collection<ClusterEvent<K, V>>> events) {
      super(cacheName);
      this.multiEvents = events;
   }

   @ProtoFactory
   MultiClusterEventCommand(ByteString cacheName, MarshallableMap<UUID, MarshallableCollection<ClusterEvent<K, V>>> events) {
      super(cacheName);
      this.multiEvents = MarshallableMap.unwrap(events).entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().get()));
   }

   // TODO remove this monstrosity
   @ProtoField(number = 2)
   MarshallableMap<UUID, MarshallableCollection<ClusterEvent<K, V>>> getEvents() {
      Map<UUID, MarshallableCollection<ClusterEvent<K, V>>> map = multiEvents.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey,
                  e -> MarshallableCollection.create(e.getValue())));
      return MarshallableMap.create(map);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) {
      if (log.isTraceEnabled()) {
         log.tracef("Received multiple cluster event(s) %s", multiEvents);
      }
      AggregateCompletionStage<Void> innerComposed = CompletionStages.aggregateCompletionStage();
      for (Entry<UUID, Collection<ClusterEvent<K, V>>> event : multiEvents.entrySet()) {
         UUID identifier = event.getKey();
         Collection<ClusterEvent<K, V>> events = event.getValue();
         for (ClusterEvent<K, V> ce : events) {
            ce.cache = componentRegistry.getCache().wired();
         }
         ClusterCacheNotifier<K, V> clusterCacheNotifier = componentRegistry.getClusterCacheNotifier().running();
         innerComposed.dependsOn(clusterCacheNotifier.notifyClusterListeners(events, identifier));
      }
      return innerComposed.freeze();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }
}
