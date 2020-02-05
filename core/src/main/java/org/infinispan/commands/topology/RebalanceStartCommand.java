package org.infinispan.commands.topology;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.PersistentUUID;

/**
 * The coordinator is starting a rebalance operation.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class RebalanceStartCommand extends AbstractCacheControlCommand {

   public static final byte COMMAND_ID = 92;

   private String cacheName;
   private ConsistentHash currentCH;
   private ConsistentHash pendingCH;
   private CacheTopology.Phase phase;
   private List<Address> actualMembers;
   private List<PersistentUUID> persistentUUIDs;
   private int rebalanceId;
   private int topologyId;
   private int viewId;

   // For CommandIdUniquenessTest only
   public RebalanceStartCommand() {
      super(COMMAND_ID);
   }

   public RebalanceStartCommand(String cacheName, Address origin, CacheTopology cacheTopology, int viewId) {
      super(COMMAND_ID, origin);
      this.cacheName = cacheName;
      this.topologyId = cacheTopology.getTopologyId();
      this.rebalanceId = cacheTopology.getRebalanceId();
      this.currentCH = cacheTopology.getCurrentCH();
      this.pendingCH = cacheTopology.getPendingCH();
      this.phase = cacheTopology.getPhase();
      this.actualMembers = cacheTopology.getActualMembers();
      this.persistentUUIDs = cacheTopology.getMembersPersistentUUIDs();
      this.viewId = viewId;
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      CacheTopology topology = new CacheTopology(topologyId, rebalanceId, currentCH, pendingCH, phase, actualMembers, persistentUUIDs);
      return gcr.getLocalTopologyManager()
            .handleRebalance(cacheName, topology, viewId, origin);
   }

   public String getCacheName() {
      return cacheName;
   }

   public ConsistentHash getCurrentCH() {
      return currentCH;
   }

   public ConsistentHash getPendingCH() {
      return pendingCH;
   }

   public CacheTopology.Phase getPhase() {
      return phase;
   }

   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallString(cacheName, output);
      output.writeObject(currentCH);
      output.writeObject(pendingCH);
      MarshallUtil.marshallEnum(phase, output);
      MarshallUtil.marshallCollection(actualMembers, output);
      MarshallUtil.marshallCollection(persistentUUIDs, output);
      output.writeInt(topologyId);
      output.writeInt(rebalanceId);
      output.writeInt(viewId);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      cacheName = MarshallUtil.unmarshallString(input);
      currentCH = (ConsistentHash) input.readObject();
      pendingCH = (ConsistentHash) input.readObject();
      phase = MarshallUtil.unmarshallEnum(input, CacheTopology.Phase::valueOf);
      actualMembers = MarshallUtil.unmarshallCollection(input, ArrayList::new);
      persistentUUIDs = MarshallUtil.unmarshallCollection(input, ArrayList::new);
      topologyId = input.readInt();
      rebalanceId = input.readInt();
      viewId = input.readInt();
   }

   @Override
   public String toString() {
      return "RebalanceStartCommand{" +
            "cacheName='" + cacheName + '\'' +
            ", origin=" + origin +
            ", currentCH=" + currentCH +
            ", pendingCH=" + pendingCH +
            ", phase=" + phase +
            ", actualMembers=" + actualMembers +
            ", persistentUUIDs=" + persistentUUIDs +
            ", rebalanceId=" + rebalanceId +
            ", topologyId=" + topologyId +
            ", viewId=" + viewId +
            '}';
   }
}
