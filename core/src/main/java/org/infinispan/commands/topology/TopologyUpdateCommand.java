package org.infinispan.commands.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.marshall.protostream.impl.WrappedMessages;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.PersistentUUID;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Coordinator to member:
 * The coordinator is updating the consistent hash.
 * Used to signal the end of rebalancing as well.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.TOPOLOGY_UPDATE_COMMAND)
public class TopologyUpdateCommand extends AbstractCacheControlCommand {
   private static final Log log = LogFactory.getLog(TopologyUpdateCommand.class);

   public static final byte COMMAND_ID = 95;

   @ProtoField(number = 1)
   final String cacheName;

   @ProtoField(number = 2)
   final WrappedMessage currentCH;

   @ProtoField(number = 3)
   final WrappedMessage pendingCH;

   @ProtoField(number = 4)
   final CacheTopology.Phase phase;

   @ProtoField(number = 5)
   final List<JGroupsAddress> actualMembers;

   @ProtoField(number = 6, collectionImplementation = ArrayList.class)
   final List<PersistentUUID> persistentUUIDs;

   @ProtoField(number = 7)
   final AvailabilityMode availabilityMode;

   @ProtoField(number = 8, defaultValue = "-1")
   final int rebalanceId;

   @ProtoField(number = 9, defaultValue = "-1")
   final int topologyId;

   @ProtoField(number = 10, defaultValue = "-1")
   final int viewId;

   @ProtoFactory
   TopologyUpdateCommand(String cacheName, WrappedMessage currentCH, WrappedMessage pendingCH,
                         CacheTopology.Phase phase, List<JGroupsAddress> actualMembers,
                         List<PersistentUUID> persistentUUIDs, AvailabilityMode availabilityMode,
                         int rebalanceId, int topologyId, int viewId) {
      super(COMMAND_ID);
      this.cacheName = cacheName;
      this.currentCH = currentCH;
      this.pendingCH = pendingCH;
      this.phase = phase;
      this.actualMembers = actualMembers;
      this.persistentUUIDs = persistentUUIDs;
      this.availabilityMode = availabilityMode;
      this.rebalanceId = rebalanceId;
      this.topologyId = topologyId;
      this.viewId = viewId;
   }

   public TopologyUpdateCommand(String cacheName, Address origin, CacheTopology cacheTopology,
                                AvailabilityMode availabilityMode, int viewId) {
      super(COMMAND_ID, origin);
      this.cacheName = cacheName;
      this.topologyId = cacheTopology.getTopologyId();
      this.rebalanceId = cacheTopology.getRebalanceId();
      this.currentCH = new WrappedMessage(cacheTopology.getCurrentCH());
      this.pendingCH = new WrappedMessage(cacheTopology.getPendingCH());
      this.phase = cacheTopology.getPhase();
      this.availabilityMode = availabilityMode;
      this.actualMembers = (List<JGroupsAddress>)(List<?>) cacheTopology.getActualMembers();
      this.persistentUUIDs = cacheTopology.getMembersPersistentUUIDs();
      this.viewId = viewId;
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      if (!gcr.isLocalTopologyManagerRunning()) {
         log.debugf("Discard topology update because topology manager not running %s", this);
         return CompletableFutures.completedNull();
      }

      CacheTopology topology = new CacheTopology(topologyId, rebalanceId, getCurrentCH(), getPendingCH(), phase,
            (List<Address>)(List<?>) actualMembers, persistentUUIDs);
      return gcr.getLocalTopologyManager()
            .handleTopologyUpdate(cacheName, topology, availabilityMode, viewId, origin);
   }

   public String getCacheName() {
      return cacheName;
   }

   public ConsistentHash getCurrentCH() {
      return WrappedMessages.unwrap(currentCH);
   }

   public ConsistentHash getPendingCH() {
      return WrappedMessages.unwrap(pendingCH);
   }

   public CacheTopology.Phase getPhase() {
      return phase;
   }

   public AvailabilityMode getAvailabilityMode() {
      return availabilityMode;
   }

   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public String toString() {
      return "TopologyUpdateCommand{" +
            "cacheName='" + cacheName + '\'' +
            ", origin=" + origin +
            ", currentCH=" + getCurrentCH() +
            ", pendingCH=" + getPendingCH() +
            ", phase=" + phase +
            ", actualMembers=" + actualMembers +
            ", persistentUUIDs=" + persistentUUIDs +
            ", availabilityMode=" + availabilityMode +
            ", rebalanceId=" + rebalanceId +
            ", topologyId=" + topologyId +
            ", viewId=" + viewId +
            '}';
   }
}
