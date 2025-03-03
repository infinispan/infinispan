package org.infinispan.notifications.cachelistener.cluster;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;
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

   private static final Log log = LogFactory.getLog(MultiClusterEventCommand.class);

   private final Map<UUID, Collection<ClusterEvent<K, V>>> multiEvents;

   public MultiClusterEventCommand(ByteString cacheName, Map<UUID, Collection<ClusterEvent<K, V>>> events) {
      super(cacheName);
      this.multiEvents = events;
   }

   @ProtoFactory
   MultiClusterEventCommand(ByteString cacheName, Stream<UUIDMap<K, V>> events) {
      super(cacheName);
      this.multiEvents = events.collect(Collectors.toMap(e -> e.uuid, e -> e.events));
   }

   @ProtoField(2)
   Stream<UUIDMap<K, V>> getEvents() {
      return multiEvents.entrySet()
            .stream()
            .map(e -> new UUIDMap<>(e.getKey(), e.getValue()));
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
   public boolean isReturnValueExpected() {
      return false;
   }

   @ProtoTypeId(ProtoStreamTypeIds.MULTI_CLUSTER_EVENT_COMMAND_UUID_MAP)
   public static class UUIDMap<K, V> {

      @ProtoField(1)
      final UUID uuid;

      @ProtoField(2)
      final Collection<ClusterEvent<K, V>> events;

      @ProtoFactory
      UUIDMap(UUID uuid, Collection<ClusterEvent<K, V>> events) {
         this.uuid = uuid;
         this.events = events;
      }
   }
}
