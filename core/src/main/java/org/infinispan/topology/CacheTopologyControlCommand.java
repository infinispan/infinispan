package org.infinispan.topology;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A control command for all cache membership/rebalance operations.
 * It is not a {@code CacheRpcCommand} because it needs to run on the coordinator even when
 * the coordinator doesn't have a certain cache running.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class CacheTopologyControlCommand implements ReplicableCommand {

   public enum Type {
      // Member to coordinator:
      // A node is requesting to join the cluster.
      JOIN,
      // A member is signaling that it wants to leave the cluster.
      LEAVE,
      // A member is confirming that it has finished the topology change during rebalance.
      REBALANCE_PHASE_CONFIRM,
      // A member is requesting a cache shutdown
      SHUTDOWN_REQUEST,

      // Coordinator to member:
      // The coordinator is updating the consistent hash.
      // Used to signal the end of rebalancing as well.
      CH_UPDATE,
      // The coordinator is starting a rebalance operation.
      REBALANCE_START,
      // The coordinator is requesting information about the running caches.
      GET_STATUS,
      // Update the stable topology
      STABLE_TOPOLOGY_UPDATE,
      // Tell members to shutdown cache
      SHUTDOWN_PERFORM,

      // Member to coordinator:
      // Enable/disable rebalancing, check whether rebalancing is enabled
      POLICY_DISABLE,
      POLICY_ENABLE,
      POLICY_GET_STATUS,
      // Change the availability
      AVAILABILITY_MODE_CHANGE,
      // Query the rebalancing progress
      REBALANCING_GET_STATUS;

      private static final Type[] CACHED_VALUES = values();
   }

   private static final Log log = LogFactory.getLog(CacheTopologyControlCommand.class);

   public static final byte COMMAND_ID = 17;

   private transient LocalTopologyManager localTopologyManager;
   private transient ClusterTopologyManager clusterTopologyManager;

   private String cacheName;
   private Type type;
   private Address sender;
   private CacheJoinInfo joinInfo;

   private int topologyId;
   private int rebalanceId;
   private ConsistentHash currentCH;
   private ConsistentHash pendingCH;
   private AvailabilityMode availabilityMode;
   private List<Address> actualMembers;
   private List<PersistentUUID> persistentUUIDs;

   private Throwable throwable;
   private int viewId;

   // For CommandIdUniquenessTest only
   public CacheTopologyControlCommand() {
      this.cacheName = null;
   }

   public CacheTopologyControlCommand(String cacheName, Type type, Address sender, int viewId) {
      this.cacheName = cacheName;
      this.type = type;
      this.sender = sender;
      this.viewId = viewId;
   }

   public CacheTopologyControlCommand(String cacheName, Type type, Address sender, CacheJoinInfo joinInfo, int viewId) {
      this.cacheName = cacheName;
      this.type = type;
      this.sender = sender;
      this.joinInfo = joinInfo;
      this.viewId = viewId;
   }

   public CacheTopologyControlCommand(String cacheName, Type type, Address sender, int topologyId, int rebalanceId,
                                      Throwable throwable, int viewId) {
      this.cacheName = cacheName;
      this.type = type;
      this.sender = sender;
      this.topologyId = topologyId;
      this.rebalanceId = rebalanceId;
      this.throwable = throwable;
      this.viewId = viewId;
   }

   public CacheTopologyControlCommand(String cacheName, Type type, Address sender, AvailabilityMode availabilityMode,
         int viewId) {
      this.cacheName = cacheName;
      this.type = type;
      this.sender = sender;
      this.availabilityMode = availabilityMode;
      this.viewId = viewId;
   }

   public CacheTopologyControlCommand(String cacheName, Type type, Address sender, CacheTopology cacheTopology,
         AvailabilityMode availabilityMode, int viewId) {
      this.cacheName = cacheName;
      this.type = type;
      this.sender = sender;
      this.topologyId = cacheTopology.getTopologyId();
      this.rebalanceId = cacheTopology.getRebalanceId();
      this.currentCH = cacheTopology.getCurrentCH();
      this.pendingCH = cacheTopology.getPendingCH();
      this.availabilityMode = availabilityMode;
      this.actualMembers = cacheTopology.getActualMembers();
      this.persistentUUIDs = cacheTopology.getMembersPersistentUUIDs();
      this.viewId = viewId;
   }

   @Inject
   public void init(LocalTopologyManager localTopologyManager, ClusterTopologyManager clusterTopologyManager) {
      this.localTopologyManager = localTopologyManager;
      this.clusterTopologyManager = clusterTopologyManager;
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      final boolean trace = log.isTraceEnabled();
      LogFactory.pushNDC(cacheName, trace);
      try {
         Object responseValue = doPerform();
         return CompletableFuture.completedFuture(SuccessfulResponse.create(responseValue));
      } catch (InterruptedException e) {
         log.tracef("Command execution %s was interrupted because the cache manager is shutting down", this);
         return CompletableFuture.completedFuture(UnsuccessfulResponse.INSTANCE);
      } catch (Exception t) {
         log.exceptionHandlingCommand(this, t);
         // todo [anistor] CommandAwareRequestDispatcher does not wrap our exceptions so we have to do it instead
         return CompletableFuture.completedFuture(new ExceptionResponse(t));
      } finally {
         LogFactory.popNDC(trace);
      }
   }

   private Object doPerform() throws Exception {
      switch (type) {
         // member to coordinator
         case JOIN:
            return clusterTopologyManager.handleJoin(cacheName, sender, joinInfo, viewId);
         case LEAVE:
            clusterTopologyManager.handleLeave(cacheName, sender, viewId);
            return null;
         case REBALANCE_PHASE_CONFIRM:
            clusterTopologyManager.handleRebalancePhaseConfirm(cacheName, sender, topologyId, throwable, viewId);
            return null;
         case SHUTDOWN_REQUEST:
            clusterTopologyManager.handleShutdownRequest(cacheName);
            return null;

         // coordinator to member
         case CH_UPDATE:
            localTopologyManager.handleTopologyUpdate(cacheName, new CacheTopology(topologyId, rebalanceId, currentCH,
                  pendingCH, actualMembers, persistentUUIDs), availabilityMode, viewId, sender);
            return null;
         case STABLE_TOPOLOGY_UPDATE:
            localTopologyManager.handleStableTopologyUpdate(cacheName, new CacheTopology(topologyId, rebalanceId,
                  currentCH, pendingCH, actualMembers, persistentUUIDs), sender, viewId);
            return null;
         case REBALANCE_START:
            localTopologyManager.handleRebalance(cacheName, new CacheTopology(topologyId, rebalanceId, currentCH,
                  pendingCH, actualMembers, persistentUUIDs), viewId, sender);
            return null;
         case GET_STATUS:
            return localTopologyManager.handleStatusRequest(viewId);
         case SHUTDOWN_PERFORM:
            localTopologyManager.handleCacheShutdown(cacheName);
            return null;

         // rebalance policy control
         case POLICY_GET_STATUS:
            return clusterTopologyManager.isRebalancingEnabled(cacheName);
         case POLICY_ENABLE:
            clusterTopologyManager.setRebalancingEnabled(cacheName, true);
            return true;
         case POLICY_DISABLE:
            clusterTopologyManager.setRebalancingEnabled(cacheName, false);
            return true;

         // availability mode
         case AVAILABILITY_MODE_CHANGE:
            clusterTopologyManager.forceAvailabilityMode(cacheName, availabilityMode);
            return true;

         // rebalancing status
         case REBALANCING_GET_STATUS:
            return clusterTopologyManager.getRebalancingStatus(cacheName);
         default:
            throw new CacheException("Unknown cache topology control command type " + type);
      }
   }

   public String getCacheName() {
      return cacheName;
   }

   public Address getOrigin() {
      return sender;
   }

   public Type getType() {
      return type;
   }

   public int getTopologyId() {
      return topologyId;
   }

   public ConsistentHash getCurrentCH() {
      return currentCH;
   }

   public ConsistentHash getPendingCH() {
      return pendingCH;
   }

   public AvailabilityMode getAvailabilityMode() {
      return availabilityMode;
   }

   public Throwable getThrowable() {
      return throwable;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallString(cacheName, output);
      MarshallUtil.marshallEnum(type, output);
      switch (type) {
         case JOIN:
            output.writeObject(sender);
            output.writeObject(joinInfo);
            output.writeInt(viewId);
            return;
         case LEAVE:
            output.writeObject(sender);
            output.writeInt(viewId);
            return;
         case REBALANCE_PHASE_CONFIRM:
            output.writeObject(sender);
            output.writeObject(throwable);
            output.writeInt(viewId);
            output.writeInt(topologyId);
            return;
         case CH_UPDATE:
            output.writeObject(sender);
            output.writeObject(currentCH);
            output.writeObject(pendingCH);
            MarshallUtil.marshallCollection(actualMembers, output);
            MarshallUtil.marshallCollection(persistentUUIDs, output);
            MarshallUtil.marshallEnum(availabilityMode, output);
            output.writeInt(topologyId);
            output.writeInt(rebalanceId);
            output.writeInt(viewId);
            return;
         case STABLE_TOPOLOGY_UPDATE:
            output.writeObject(sender);
            output.writeObject(currentCH);
            output.writeObject(pendingCH);
            MarshallUtil.marshallCollection(actualMembers, output);
            MarshallUtil.marshallCollection(persistentUUIDs, output);
            output.writeInt(topologyId);
            output.writeInt(rebalanceId);
            output.writeInt(viewId);
            return;
         case REBALANCE_START:
            output.writeObject(sender);
            output.writeObject(currentCH);
            output.writeObject(pendingCH);
            MarshallUtil.marshallCollection(actualMembers, output);
            MarshallUtil.marshallCollection(persistentUUIDs, output);
            output.writeInt(topologyId);
            output.writeInt(rebalanceId);
            output.writeInt(viewId);
            return;
         case GET_STATUS:
            output.writeInt(viewId);
            return;
         case AVAILABILITY_MODE_CHANGE:
            MarshallUtil.marshallEnum(availabilityMode, output);
            return;
         case POLICY_GET_STATUS:
         case POLICY_ENABLE:
         case POLICY_DISABLE:
         case REBALANCING_GET_STATUS:
         default:
      }
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      cacheName = MarshallUtil.unmarshallString(input);
      type = MarshallUtil.unmarshallEnum(input, ordinal -> Type.CACHED_VALUES[ordinal]);
      switch (type) {
         case JOIN:
            sender = (Address) input.readObject();
            joinInfo = (CacheJoinInfo) input.readObject();
            viewId = input.readInt();
            return;
         case LEAVE:
            sender = (Address) input.readObject();
            viewId = input.readInt();
            return;
         case REBALANCE_PHASE_CONFIRM:
            sender = (Address) input.readObject();
            throwable = (Throwable) input.readObject();
            viewId = input.readInt();
            topologyId = input.readInt();
            return;
         case CH_UPDATE:
            sender = (Address) input.readObject();
            currentCH = (ConsistentHash) input.readObject();
            pendingCH = (ConsistentHash) input.readObject();
            actualMembers = MarshallUtil.unmarshallCollection(input, ArrayList::new);
            persistentUUIDs = MarshallUtil.unmarshallCollection(input, ArrayList::new);
            availabilityMode = MarshallUtil.unmarshallEnum(input, AvailabilityMode::valueOf);
            topologyId = input.readInt();
            rebalanceId = input.readInt();
            viewId = input.readInt();
            return;
         case STABLE_TOPOLOGY_UPDATE:
            sender = (Address) input.readObject();
            currentCH = (ConsistentHash) input.readObject();
            pendingCH = (ConsistentHash) input.readObject();
            actualMembers = MarshallUtil.unmarshallCollection(input, ArrayList::new);
            persistentUUIDs = MarshallUtil.unmarshallCollection(input, ArrayList::new);
            topologyId = input.readInt();
            rebalanceId = input.readInt();
            viewId = input.readInt();
            return;
         case REBALANCE_START:
            sender = (Address) input.readObject();
            currentCH = (ConsistentHash) input.readObject();
            pendingCH = (ConsistentHash) input.readObject();
            actualMembers = MarshallUtil.unmarshallCollection(input, ArrayList::new);
            persistentUUIDs = MarshallUtil.unmarshallCollection(input, ArrayList::new);
            topologyId = input.readInt();
            rebalanceId = input.readInt();
            viewId = input.readInt();
            return;
         case GET_STATUS:
            viewId = input.readInt();
            return;
         case AVAILABILITY_MODE_CHANGE:
            availabilityMode = MarshallUtil.unmarshallEnum(input, AvailabilityMode::valueOf);
            return;
         case POLICY_GET_STATUS:
         case POLICY_ENABLE:
         case POLICY_DISABLE:
         case REBALANCING_GET_STATUS:
         default:
      }
   }

   @Override
   public String toString() {
      return "CacheTopologyControlCommand{" +
            "cache=" + cacheName +
            ", type=" + type +
            ", sender=" + sender +
            ", joinInfo=" + joinInfo +
            ", topologyId=" + topologyId +
            ", rebalanceId=" + rebalanceId +
            ", currentCH=" + currentCH +
            ", pendingCH=" + pendingCH +
            ", availabilityMode=" + availabilityMode +
            ", actualMembers=" + actualMembers +
            ", throwable=" + throwable +
            ", viewId=" + viewId +
            '}';
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return true;
   }
}
