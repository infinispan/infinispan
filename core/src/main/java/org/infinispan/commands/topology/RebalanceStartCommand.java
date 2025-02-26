package org.infinispan.commands.topology;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.PersistentUUID;

/**
 * The coordinator is starting a rebalance operation.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@ProtoTypeId(ProtoStreamTypeIds.REBALANCE_START_COMMAND)
public class RebalanceStartCommand extends AbstractCacheControlCommand {

   @ProtoField(1)
   final String cacheName;

   @ProtoField(2)
   final WrappedMessage currentCH;

   @ProtoField(3)
   final WrappedMessage pendingCH;

   @ProtoField(4)
   final CacheTopology.Phase phase;

   @ProtoField(5)
   final List<PersistentUUID> persistentUUIDs;

   @ProtoField(6)
   final int rebalanceId;

   @ProtoField(7)
   final int topologyId;

   @ProtoField(8)
   final int viewId;

   private List<Address> actualMembers;

   @ProtoFactory
   RebalanceStartCommand(String cacheName, WrappedMessage currentCH, WrappedMessage pendingCH,
                                CacheTopology.Phase phase, List<PersistentUUID> persistentUUIDs,
                                int rebalanceId, int topologyId, int viewId, List<JGroupsAddress> actualMembers) {
      this.cacheName = cacheName;
      this.currentCH = currentCH;
      this.pendingCH = pendingCH;
      this.phase = phase;
      this.persistentUUIDs = persistentUUIDs;
      this.rebalanceId = rebalanceId;
      this.topologyId = topologyId;
      this.viewId = viewId;
      this.actualMembers = (List<Address>)(List<?>) actualMembers;
   }

   public RebalanceStartCommand(String cacheName, Address origin, CacheTopology cacheTopology, int viewId) {
      super(origin);
      this.cacheName = cacheName;
      this.topologyId = cacheTopology.getTopologyId();
      this.rebalanceId = cacheTopology.getRebalanceId();
      this.currentCH = new WrappedMessage(cacheTopology.getCurrentCH());
      this.pendingCH = new WrappedMessage(cacheTopology.getPendingCH());
      this.phase = cacheTopology.getPhase();
      this.actualMembers = cacheTopology.getActualMembers();
      this.persistentUUIDs = cacheTopology.getMembersPersistentUUIDs();
      this.viewId = viewId;
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      CacheTopology topology = new CacheTopology(topologyId, rebalanceId, getCurrentCH(), getPendingCH(), phase, actualMembers, persistentUUIDs);
      return gcr.getLocalTopologyManager()
            .handleRebalance(cacheName, topology, viewId, origin);
   }

   @ProtoField(9)
   List<JGroupsAddress> getActualMembers() {
      return (List<JGroupsAddress>)(List<?>) actualMembers;
   }

   public String getCacheName() {
      return cacheName;
   }

   public ConsistentHash getCurrentCH() {
      return (ConsistentHash) currentCH.getValue();
   }

   public ConsistentHash getPendingCH() {
      return (ConsistentHash) pendingCH.getValue();
   }

   public CacheTopology.Phase getPhase() {
      return phase;
   }

   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public String toString() {
      return "RebalanceStartCommand{" +
            "cacheName='" + cacheName + '\'' +
            ", origin=" + origin +
            ", currentCH=" + getCurrentCH() +
            ", pendingCH=" + getPendingCH() +
            ", phase=" + phase +
            ", actualMembers=" + actualMembers +
            ", persistentUUIDs=" + persistentUUIDs +
            ", rebalanceId=" + rebalanceId +
            ", topologyId=" + topologyId +
            ", viewId=" + viewId +
            '}';
   }
}
