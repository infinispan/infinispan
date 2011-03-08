package org.infinispan.remoting.transport;

/**
 * Wraps a TopologyUUID JGroups address
 *
 * @author bela
 * @since 4.0
 */
public interface TopologyAwareAddress extends Address {
   boolean isSameSite(TopologyAwareAddress addr);
   boolean isSameRack(TopologyAwareAddress addr);
   boolean isSameMachine(TopologyAwareAddress addr);
}
