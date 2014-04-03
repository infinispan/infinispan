package org.infinispan.remoting.transport.jgroups;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.TopologyAwareAddress;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

/**
 * An encapsulation of a JGroups Address
 *
 * @author Bela Ban
 * @since 5.0
 */
public final class JGroupsTopologyAwareAddress extends JGroupsAddress implements TopologyAwareAddress {

   protected static final byte[] SITE_ID    = Util.stringToBytes("site-id");
   protected static final byte[] RACK_ID    = Util.stringToBytes("rack-id");
   protected static final byte[] MACHINE_ID = Util.stringToBytes("machine-id");

   private final ExtendedUUID topologyAddress;


   public JGroupsTopologyAwareAddress(ExtendedUUID address) {
      super(address);
      topologyAddress = address;
   }


   @Override
   public String getSiteId() {
      return Util.bytesToString(topologyAddress.get(SITE_ID));
   }

   @Override
   public String getRackId() {
      return Util.bytesToString(topologyAddress.get(RACK_ID));
   }

   @Override
   public String getMachineId() {
      return Util.bytesToString(topologyAddress.get(MACHINE_ID));
   }


   @Override
   public boolean isSameSite(TopologyAwareAddress addr) {
      return getSiteId() == null ? addr.getSiteId() == null : getSiteId().equals(addr.getSiteId());
   }

   @Override
   public boolean isSameRack(TopologyAwareAddress addr) {
      if (!isSameSite(addr))
         return false;
      return getRackId() == null ? addr.getRackId() == null : getRackId().equals(addr.getRackId());
   }

   @Override
   public boolean isSameMachine(TopologyAwareAddress addr) {
      if (!isSameRack(addr))
         return false;
      return getMachineId() == null ? addr.getMachineId() == null : getMachineId().equals(addr.getMachineId());
   }

   public static final class Externalizer implements AdvancedExternalizer<JGroupsTopologyAwareAddress> {
      @Override
      public void writeObject(ObjectOutput output, JGroupsTopologyAwareAddress address) throws IOException {
         try {
            org.jgroups.util.Util.writeAddress(address.address, output);
         } catch (Exception e) {
            throw new IOException(e);
         }
      }

      @Override
      public JGroupsTopologyAwareAddress readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         try {
            ExtendedUUID jgroupsAddress = (ExtendedUUID) org.jgroups.util.Util.readAddress(unmarshaller);
            return new JGroupsTopologyAwareAddress(jgroupsAddress);
         } catch (Exception e) {
            throw new IOException(e);
         }
      }

      @Override
      public Set<Class<? extends JGroupsTopologyAwareAddress>> getTypeClasses() {
         return Collections.<Class<? extends JGroupsTopologyAwareAddress>>singleton(JGroupsTopologyAwareAddress.class);
      }

      @Override
      public Integer getId() {
         return Ids.JGROUPS_TOPOLOGY_AWARE_ADDRESS;
      }
   }
}
