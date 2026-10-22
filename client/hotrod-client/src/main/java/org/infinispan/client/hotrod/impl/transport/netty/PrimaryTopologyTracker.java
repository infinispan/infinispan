package org.infinispan.client.hotrod.impl.transport.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.topology.CacheInfo;

import io.netty.channel.Channel;

/**
 * Orders primary listener snapshots and coordinates registration readiness across connection replacements.
 * All access is guarded by this tracker's monitor. Topology publication and listener notifications remain
 * under that monitor, with the topology StampedLock acquired only after the monitor.
 */
final class PrimaryTopologyTracker {
   private final OperationDispatcher dispatcher;
   private final Map<String, PrimaryTopology> primaryTopologies = new HashMap<>();

   PrimaryTopologyTracker(OperationDispatcher dispatcher) {
      this.dispatcher = dispatcher;
   }

   synchronized void addListener(String cacheName, NearCacheTopologyListener listener) {
      primaryTopologies.computeIfAbsent(cacheName, ignored -> new PrimaryTopology()).listeners.add(listener);
   }

   synchronized void removeListener(String cacheName, NearCacheTopologyListener listener) {
      PrimaryTopology topology = primaryTopologies.get(cacheName);
      if (topology != null && topology.listeners.remove(listener) && topology.listeners.isEmpty()) {
         primaryTopologies.remove(cacheName);
         topology.ready.values().forEach(future -> future.completeExceptionally(
               new IllegalStateException("Primary listener removed")));
      }
   }

   synchronized List<NearCacheTopologyListener> listeners(String cacheName) {
      PrimaryTopology topology = primaryTopologies.get(cacheName);
      return topology == null ? List.of() : topology.listeners;
   }

   private static final class PrimaryTopology {
      final List<NearCacheTopologyListener> listeners = new CopyOnWriteArrayList<>();
      int sequence = -1;
      final Set<Channel> channels = new HashSet<>();
      final Map<Channel, PrimaryTopologyUpdate> pending = new HashMap<>();
      final Map<Channel, CompletableFuture<Channel>> ready = new HashMap<>();
   }

   private record PrimaryTopologyUpdate(Channel channel, int sequence, int baseSequence, BitSet changedSegments,
                                        int hash, InetSocketAddress[] addresses, SocketAddress[][] segmentOwners,
                                        short hashFunctionVersion) { }

   /** Returns the listener channel once it has supplied an accepted topology snapshot. */
   synchronized CompletionStage<Channel> primaryTopologyReady(String cacheName, Channel channel) {
      if (!dispatcher.isCurrentChannel(channel)) {
         return CompletableFuture.failedFuture(new IllegalStateException("Primary listener channel retired"));
      }
      Map<Channel, CompletableFuture<Channel>> ready =
            primaryTopologies.computeIfAbsent(cacheName, ignored -> new PrimaryTopology()).ready;
      CompletableFuture<Channel> future = ready.computeIfAbsent(channel, ignored -> new CompletableFuture<>());
      if (future.isCompletedExceptionally()) {
         // A rejected snapshot arrived before anyone asked. Report it once, so a retry waits for a new snapshot.
         ready.remove(channel);
      }
      return future;
   }

   /**
    * Applies a sequenced listener push only while its source is a current connection. The server topology
    * {@code sequence} is the only ordering authority, while {@code topologyHash} just identifies the consistent hash.
    * The push carries the segments whose primary owner changed since {@code baseSequence}, which is the sequence
    * the server last sent on that connection, or {@code -1} when the connection has no earlier history.
    */
   synchronized void updatePrimaryTopology(String cacheName, Channel channel, int sequence, int baseSequence,
                                           BitSet changedSegments, int topologyHash,
                                           InetSocketAddress[] addresses, SocketAddress[][] segmentOwners,
                                           short hashFunctionVersion) {
      if (!dispatcher.isCurrentChannel(channel)) {
         return;
      }
      PrimaryTopology topology = primaryTopologies.computeIfAbsent(cacheName, ignored -> new PrimaryTopology());
      resumePrimaryTopology(cacheName, topology);
      acceptPrimaryTopology(cacheName, topology,
            new PrimaryTopologyUpdate(channel, sequence, baseSequence, changedSegments, topologyHash, addresses,
                  segmentOwners, hashFunctionVersion));
   }

   private void prunePrimaryChannels(PrimaryTopology topology) {
      topology.channels.removeIf(channel -> !dispatcher.isCurrentChannel(channel));
      topology.pending.keySet().removeIf(channel -> !dispatcher.isCurrentChannel(channel));
      if (topology.channels.isEmpty()) {
         topology.sequence = -1;
      }
   }

   private void acceptPrimaryTopology(String cacheName, PrimaryTopology topology, PrimaryTopologyUpdate update) {
      if (!dispatcher.isCurrentChannel(update.channel)) {
         return;
      }
      if (update.sequence < topology.sequence) {
         // A replacement connection can report its reset sequence before the remaining old sockets
         // disconnect. Keep its snapshot until those sockets retire, rather than resetting on every reconnect.
         if (!topology.channels.contains(update.channel)) {
            topology.pending.compute(update.channel, (channel, previous) ->
                  previous == null || update.sequence > previous.sequence ? update : previous);
         }
         return;
      }
      if (update.sequence > topology.sequence) {
         // The segments are fenced before the topology is published, even if it has the same owners as the current
         // one, as owners can change and change back between two accepted pushes. The history of the push is only
         // trusted when it starts at or before the accepted sequence. Otherwise any segment could have changed
         // unnoticed, so all of them are fenced. Nothing is fenced for the first sequence accepted, as the
         // listener activation reconciles against the latest accepted topology.
         boolean covered = update.baseSequence >= 0 && update.baseSequence <= topology.sequence;
         Runnable fence = topology.sequence >= 0 && (!covered || !update.changedSegments.isEmpty())
               ? () -> dispatcher.notifyPrimaryChange(cacheName, covered ? update.changedSegments : null)
               : null;
         if (!dispatcher.applyTopology(cacheName, null, update.hash, update.addresses, update.segmentOwners, update.hashFunctionVersion, fence)) {
            // The server sends the registration snapshot only once, so fail the registration rather than leave it waiting.
            CompletableFuture<Channel> ready = topology.ready.remove(update.channel);
            if (ready == null) {
               ready = new CompletableFuture<>();
               topology.ready.put(update.channel, ready);
            }
            ready.completeExceptionally(new IllegalStateException("Primary topology snapshot rejected during cluster switch"));
            return;
         }
         topology.sequence = update.sequence;
         // Include all existing sockets, even listeners whose initial snapshot has not arrived yet.
         for (SocketAddress server : dispatcher.getCacheInfo(cacheName).getServers()) {
            OperationChannel connection = dispatcher.getHandlerForAddress(server);
            if (connection != null && connection.getChannel() != null && dispatcher.isCurrentChannel(connection.getChannel())
                  && !topology.pending.containsKey(connection.getChannel())) {
               topology.channels.add(connection.getChannel());
            }
         }
      }
      if (dispatcher.isCurrentChannel(update.channel)) {
         topology.channels.add(update.channel);
         topology.pending.remove(update.channel);
         topology.ready.computeIfAbsent(update.channel, ignored -> new CompletableFuture<>()).complete(update.channel);
      }
   }

   /** Removes ordering state for a closed socket and resumes snapshots waiting for the old sockets to retire. */
   synchronized void retirePrimaryChannel(Channel channel) {
      new HashMap<>(primaryTopologies).forEach((cacheName, topology) -> {
         topology.channels.remove(channel);
         topology.pending.remove(channel);
         CompletableFuture<Channel> ready = topology.ready.remove(channel);
         if (ready != null) {
            ready.completeExceptionally(new IllegalStateException("Primary listener channel retired"));
         }
         resumePrimaryTopology(cacheName, topology);
      });
   }

   private void resumePrimaryTopology(String cacheName, PrimaryTopology topology) {
      prunePrimaryChannels(topology);
      if (topology.sequence < 0 && !topology.pending.isEmpty()) {
         List<PrimaryTopologyUpdate> pending = new ArrayList<>(topology.pending.values());
         pending.sort(Comparator.comparingInt(PrimaryTopologyUpdate::sequence).reversed());
         pending.forEach(update -> acceptPrimaryTopology(cacheName, topology, update));
      }
   }

   synchronized <T> T withLatestTopology(String cacheName, Function<CacheInfo, T> action) {
      return action.apply(dispatcher.getCacheInfo(cacheName));
   }

   synchronized void updateTopology(String cacheName, HotRodOperation<?> operation, int responseTopologyId,
                                     InetSocketAddress[] addresses, SocketAddress[][] segmentOwners,
                                     short hashFunctionVersion, Channel source) {
      if (!dispatcher.isCurrentChannel(source) || !acceptsResponse(cacheName, source, addresses)) {
         return;
      }
      dispatcher.applyTopology(cacheName, operation, responseTopologyId, addresses, segmentOwners, hashFunctionVersion, null);
   }

   synchronized void updateTopology(String cacheName, HotRodOperation<?> operation, int responseTopologyId,
                                     InetSocketAddress[] addresses, SocketAddress[][] segmentOwners,
                                     short hashFunctionVersion) {
      dispatcher.applyTopology(cacheName, operation, responseTopologyId, addresses, segmentOwners, hashFunctionVersion, null);
   }

   private boolean acceptsResponse(String cacheName, Channel source, InetSocketAddress[] addresses) {
      PrimaryTopology topology = primaryTopologies.get(cacheName);
      if (topology != null) {
         if (topology.sequence >= 0 && !topology.channels.contains(source)) {
            dispatcher.closeExcludedChannels(cacheName, addresses);
         }
         resumePrimaryTopology(cacheName, topology);
         // Headers have no sequence. Established listener sockets provide ordered pushes instead.
         if (topology.sequence >= 0) {
            return false;
         }
      }
      return true;
   }
}
