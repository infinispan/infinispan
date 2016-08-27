package org.infinispan.commands;

/**
 * Some of the commands sent over the wire can only be honored by the receiver if the topology of the cluster at
 * delivery time is still 'compatible' with the topology in place at send time (eg. a 'get' command cannot execute
 * on a node that is no longer owner after state transfer took place). These commands need to be tagged with
 * the current topology id of the sender so the receiver can detect and handle topology mismatches.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public interface TopologyAffectedCommand extends ReplicableCommand {

   int getTopologyId();

   void setTopologyId(int topologyId);
}
