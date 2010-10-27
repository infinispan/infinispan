package org.infinispan.distribution.ch;

import org.infinispan.remoting.transport.Address;

import java.util.HashMap;
import java.util.Map;

/**
 * Aggregates topology information from all nodes within the cluster.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class TopologyInfo {
   
   private Map<Address, NodeTopologyInfo> address2TopologyInfo = new HashMap<Address, NodeTopologyInfo>();

   public void addNodeTopologyInfo(Address addr, NodeTopologyInfo ti) {
      address2TopologyInfo.put(addr, ti);
   }

   public boolean isSameSite(Address a1, Address a2) {
      NodeTopologyInfo info1 = address2TopologyInfo.get(a1);
      NodeTopologyInfo info2 = address2TopologyInfo.get(a2);
      return info1.sameSite(info2);
   }

   public boolean isSameRack(Address a1, Address a2) {
      NodeTopologyInfo info1 = address2TopologyInfo.get(a1);
      NodeTopologyInfo info2 = address2TopologyInfo.get(a2);
      return info1.sameRack(info2);
   }

   public boolean isSameMachine(Address a1, Address a2) {
      NodeTopologyInfo info1 = address2TopologyInfo.get(a1);
      NodeTopologyInfo info2 = address2TopologyInfo.get(a2);
      return info1.sameMachine(info2);
   }
}
