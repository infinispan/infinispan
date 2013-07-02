package org.infinispan.remoting.transport;

/**
 * Wraps a TopologyUUID JGroups address
 *
 * @author Bela Ban
 * @since 5.0
 */
public interface TopologyAwareAddress extends Address {
   String getSiteId();
   String getRackId();
   String getMachineId();

   boolean isSameSite(TopologyAwareAddress addr);
   boolean isSameRack(TopologyAwareAddress addr);
   boolean isSameMachine(TopologyAwareAddress addr);
}
