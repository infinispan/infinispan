package org.infinispan.remoting.transport.jgroups;

import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.TopologyAwareAddress;
import org.jgroups.util.TopologyUUID;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * An encapsulation of a JGroups Address
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = JGroupsTopologyAwareAddress.Externalizer.class, id = Ids.JGROUPS_TOPOLOGY_AWARE_ADDRESS)
public class JGroupsTopologyAwareAddress extends JGroupsAddress implements TopologyAwareAddress {

   public JGroupsTopologyAwareAddress() {
      super();
   }

   public JGroupsTopologyAwareAddress(org.jgroups.Address address) {
      super(address);
   }


   @Override
   public boolean isSameSite(TopologyAwareAddress addr) {
      TopologyUUID my_addr= (TopologyUUID) getJGroupsAddress();
      TopologyUUID other_addr= addr instanceof JGroupsTopologyAwareAddress?
            (TopologyUUID) ((JGroupsTopologyAwareAddress) addr).getJGroupsAddress() : null;
      return my_addr.isSameSite(other_addr);
   }

   @Override
   public boolean isSameRack(TopologyAwareAddress addr) {
      TopologyUUID my_addr= (TopologyUUID) getJGroupsAddress();
      TopologyUUID other_addr= addr instanceof JGroupsTopologyAwareAddress?
            (TopologyUUID) ((JGroupsTopologyAwareAddress) addr).getJGroupsAddress() : null;
      return my_addr.isSameRack(other_addr);
   }

   @Override
   public boolean isSameMachine(TopologyAwareAddress addr) {
      TopologyUUID my_addr= (TopologyUUID) getJGroupsAddress();
      TopologyUUID other_addr= addr instanceof JGroupsTopologyAwareAddress?
            (TopologyUUID) ((JGroupsTopologyAwareAddress) addr).getJGroupsAddress() : null;
      return my_addr.isSameMachine(other_addr);
   }

   public static class Externalizer implements org.infinispan.marshall.Externalizer {
      public void writeObject(ObjectOutput output, Object subject) throws IOException {
         JGroupsTopologyAwareAddress address = (JGroupsTopologyAwareAddress) subject;
         output.writeObject(address.address);
      }

      public Object readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         JGroupsTopologyAwareAddress address = new JGroupsTopologyAwareAddress();
         address.address = (org.jgroups.Address) unmarshaller.readObject();
         return address;
      }
   }
}
