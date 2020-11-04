package org.infinispan.notifications.cachelistener.cluster;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This command is used to send cluster events for multiple listeners on the same node
 *
 * @author wburns
 * @since 10.0
 */
public class MultiClusterEventCommand<K, V> extends BaseRpcCommand {

   public static final int COMMAND_ID = 19;

   private static final Log log = LogFactory.getLog(MultiClusterEventCommand.class);
   private final boolean trace = log.isTraceEnabled();

   private Map<UUID, Collection<ClusterEvent<K, V>>> multiEvents;

   public MultiClusterEventCommand() {
      super(null);
   }

   public MultiClusterEventCommand(ByteString cacheName) {
      super(cacheName);
   }

   public MultiClusterEventCommand(ByteString cacheName, Map<UUID, Collection<ClusterEvent<K, V>>> events) {
      super(cacheName);
      this.multiEvents = events;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) {
      if (trace) {
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

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(getOrigin());
      if (multiEvents.size() == 1) {
         output.writeBoolean(true);
         Entry entry = multiEvents.entrySet().iterator().next();
         output.writeObject(entry.getKey());
         output.writeObject(entry.getValue());
      } else {
         output.writeBoolean(false);
         output.writeObject(multiEvents);
      }
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      setOrigin((Address) input.readObject());
      boolean single = input.readBoolean();
      if (single) {
         multiEvents = Collections.singletonMap((UUID) input.readObject(), (Collection<ClusterEvent<K, V>>) input.readObject());
      } else {
         multiEvents = (Map<UUID, Collection<ClusterEvent<K, V>>>)input.readObject();
      }
   }
}
