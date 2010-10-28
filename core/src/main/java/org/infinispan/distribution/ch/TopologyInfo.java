package org.infinispan.distribution.ch;

import org.infinispan.marshall.Externalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.remoting.transport.Address;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
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

   public NodeTopologyInfo getNodeTopologyInfo(Address address) {
      return address2TopologyInfo.get(address);
   }

   public void removeNodeInfo(Address leaver) {
      address2TopologyInfo.remove(leaver);
   }

   public Collection<NodeTopologyInfo> getAllTopologyInfo() {
      return Collections.unmodifiableCollection(address2TopologyInfo.values());
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TopologyInfo that = (TopologyInfo) o;

      if (address2TopologyInfo != null ? !address2TopologyInfo.equals(that.address2TopologyInfo) : that.address2TopologyInfo != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      return address2TopologyInfo != null ? address2TopologyInfo.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "TopologyInfo{" +
            "address2TopologyInfo=" + address2TopologyInfo +
            '}';
   }
}
