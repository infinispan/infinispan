package org.infinispan.notifications.cachelistener.cluster;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.infinispan.Cache;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.AggregateCompletionStage;
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
   private static final boolean trace = log.isTraceEnabled();

   private Cache<K, V> cache;
   private ClusterCacheNotifier<K, V> clusterCacheNotifier;
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

   public void init(Cache<K, V> cache, ClusterCacheNotifier<K, V> clusterCacheNotifier) {
      this.cache = cache;
      this.clusterCacheNotifier = clusterCacheNotifier;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() {
      if (trace) {
         log.tracef("Received multiple cluster event(s) %s", multiEvents);
      }
      AggregateCompletionStage<Void> innerComposed = CompletionStages.aggregateCompletionStage();
      for (Entry<UUID, Collection<ClusterEvent<K, V>>> event : multiEvents.entrySet()) {
         UUID identifier = event.getKey();
         Collection<ClusterEvent<K, V>> events = event.getValue();
         for (ClusterEvent<K, V> ce : events) {
            ce.cache = cache;
         }
         innerComposed.dependsOn(clusterCacheNotifier.notifyClusterListeners(events, identifier));
      }
      return (CompletableFuture) innerComposed.freeze().toCompletableFuture();
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
         Entry entry = (Entry) multiEvents.entrySet().iterator().next();
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
