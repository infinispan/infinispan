package org.infinispan.topology;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.partionhandling.AvailabilityMode;
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
      // A member is confirming that it finished the rebalance operation.
      REBALANCE_CONFIRM,

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

      // Member to coordinator:
      // Enable/disable rebalancing, check whether rebalancing is enabled
      POLICY_DISABLE,
      POLICY_ENABLE,
      POLICY_GET_STATUS,
      // Change the availability
      AVAILABILITY_MODE_CHANGE,
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
      this.viewId = viewId;
   }

   @Inject
   public void init(LocalTopologyManager localTopologyManager, ClusterTopologyManager clusterTopologyManager) {
      this.localTopologyManager = localTopologyManager;
      this.clusterTopologyManager = clusterTopologyManager;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      final boolean trace = log.isTraceEnabled();
      LogFactory.pushNDC(cacheName, trace);
      try {
         Object responseValue = doPerform();
         return SuccessfulResponse.create(responseValue);
      } catch (InterruptedException e) {
         log.tracef("Command execution %s was interrupted because the cache manager is shutting down", this);
         return UnsuccessfulResponse.INSTANCE;
      } catch (Exception t) {
         log.exceptionHandlingCommand(this, t);
         // todo [anistor] CommandAwareRequestDispatcher does not wrap our exceptions so we have to do it instead
         return new ExceptionResponse(t);
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
         case REBALANCE_CONFIRM:
            clusterTopologyManager.handleRebalanceCompleted(cacheName, sender, topologyId, throwable, viewId);
            return null;

         // coordinator to member
         case CH_UPDATE:
            localTopologyManager.handleTopologyUpdate(cacheName, new CacheTopology(topologyId, rebalanceId, currentCH, pendingCH), availabilityMode, viewId);
            return null;
         case STABLE_TOPOLOGY_UPDATE:
            localTopologyManager.handleStableTopologyUpdate(cacheName, new CacheTopology(topologyId, rebalanceId, currentCH, pendingCH), viewId);
            return null;
         case REBALANCE_START:
            localTopologyManager.handleRebalance(cacheName, new CacheTopology(topologyId, rebalanceId, currentCH, pendingCH), viewId);
            return null;
         case GET_STATUS:
            return localTopologyManager.handleStatusRequest(viewId);

         // rebalance policy control
         case POLICY_GET_STATUS:
            return clusterTopologyManager.isRebalancingEnabled();
         case POLICY_ENABLE:
            clusterTopologyManager.setRebalancingEnabled(true);
            return true;
         case POLICY_DISABLE:
            clusterTopologyManager.setRebalancingEnabled(false);
            return true;

         // availability mode
         case AVAILABILITY_MODE_CHANGE:
            clusterTopologyManager.forceAvailabilityMode(cacheName, availabilityMode);
            return true;

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
   public Object[] getParameters() {
      return new Object[]{cacheName, (byte) type.ordinal(), sender, joinInfo, topologyId, rebalanceId, currentCH,
            pendingCH, availabilityMode, throwable, viewId};
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] parameters) {
      int i = 0;
      cacheName = (String) parameters[i++];
      type = Type.values()[(Byte) parameters[i++]];
      sender = (Address) parameters[i++];
      joinInfo = (CacheJoinInfo) parameters[i++];
      topologyId = (Integer) parameters[i++];
      rebalanceId = (Integer) parameters[i++];
      currentCH = (ConsistentHash) parameters[i++];
      pendingCH = (ConsistentHash) parameters[i++];
      availabilityMode = (AvailabilityMode) parameters[i++];
      throwable = (Throwable) parameters[i++];
      viewId = (Integer) parameters[i++];
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
