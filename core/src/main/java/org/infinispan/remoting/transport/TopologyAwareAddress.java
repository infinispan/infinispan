package org.infinispan.remoting.transport;

/**
 * Wraps a TopologyUUID JGroups address
 *
 * @author bela
 * @since 4.2
 */
public interface TopologyAwareAddress extends Address {
   String getSiteId();
   String getRackId();
   String getMachineId();

   boolean isSameSite(TopologyAwareAddress addr);
   boolean isSameRack(TopologyAwareAddress addr);
   boolean isSameMachine(TopologyAwareAddress addr);
}
