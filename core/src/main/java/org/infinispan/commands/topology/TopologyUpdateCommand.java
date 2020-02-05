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
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.PersistentUUID;

/**
 * Coordinator to member:
 * The coordinator is updating the consistent hash.
 * Used to signal the end of rebalancing as well.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
public class TopologyUpdateCommand extends AbstractCacheControlCommand {

   public static final byte COMMAND_ID = 95;

   private String cacheName;
   private ConsistentHash currentCH;
   private ConsistentHash pendingCH;
   private CacheTopology.Phase phase;
   private List<Address> actualMembers;
   private List<PersistentUUID> persistentUUIDs;
   private AvailabilityMode availabilityMode;
   private int rebalanceId;
   private int topologyId;
   private int viewId;

   // For CommandIdUniquenessTest only
   public TopologyUpdateCommand() {
      super(COMMAND_ID);
   }

   public TopologyUpdateCommand(String cacheName, Address origin, CacheTopology cacheTopology,
                                AvailabilityMode availabilityMode, int viewId) {
      super(COMMAND_ID, origin);
      this.cacheName = cacheName;
      this.topologyId = cacheTopology.getTopologyId();
      this.rebalanceId = cacheTopology.getRebalanceId();
      this.currentCH = cacheTopology.getCurrentCH();
      this.pendingCH = cacheTopology.getPendingCH();
      this.phase = cacheTopology.getPhase();
      this.availabilityMode = availabilityMode;
      this.actualMembers = cacheTopology.getActualMembers();
      this.persistentUUIDs = cacheTopology.getMembersPersistentUUIDs();
      this.viewId = viewId;
   }

   @Override
   public CompletionStage<?> invokeAsync(GlobalComponentRegistry gcr) throws Throwable {
      CacheTopology topology = new CacheTopology(topologyId, rebalanceId, currentCH, pendingCH, phase, actualMembers, persistentUUIDs);
      return gcr.getLocalTopologyManager()
            .handleTopologyUpdate(cacheName, topology, availabilityMode, viewId, origin);
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
      MarshallUtil.marshallEnum(availabilityMode, output);
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
      availabilityMode = MarshallUtil.unmarshallEnum(input, AvailabilityMode::valueOf);
      topologyId = input.readInt();
      rebalanceId = input.readInt();
      viewId = input.readInt();
   }

   @Override
   public String toString() {
      return "ConsistentHashUpdateCommand{" +
            "cacheName='" + cacheName + '\'' +
            ", origin=" + origin +
            ", currentCH=" + currentCH +
            ", pendingCH=" + pendingCH +
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
