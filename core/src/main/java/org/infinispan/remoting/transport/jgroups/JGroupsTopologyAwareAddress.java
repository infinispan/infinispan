package org.infinispan.remoting.transport.jgroups;

import java.io.IOException;
import java.util.Arrays;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.TopologyAwareAddress;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.NameCache;
import org.jgroups.util.Util;

/**
 * An encapsulation of a JGroups {@link ExtendedUUID} with a site id, a rack id, and a machine id.
 *
 * @author Bela Ban
 * @since 5.0
 */
@ProtoTypeId(ProtoStreamTypeIds.JGROUPS_TOPOLOGY_AWARE_ADDRESS)
public class JGroupsTopologyAwareAddress extends JGroupsAddress implements TopologyAwareAddress {
   private static final byte[] SITE_KEY = Util.stringToBytes("site-id");
   private static final byte[] RACK_KEY = Util.stringToBytes("rack-id");
   private static final byte[] MACHINE_KEY = Util.stringToBytes("machine-id");

   public static ExtendedUUID randomUUID(String name, String siteId, String rackId, String machineId) {
      ExtendedUUID uuid = ExtendedUUID.randomUUID(name);
      if (name != null) {
         NameCache.add(uuid, name);
      }
      addId(uuid, SITE_KEY, siteId);
      addId(uuid, RACK_KEY, rackId);
      addId(uuid, MACHINE_KEY, machineId);
      return uuid;
   }

   private static void addId(ExtendedUUID uuid, byte[] key, String stringValue) {
      if (stringValue != null) {
         uuid.put(key, Util.stringToBytes(stringValue));
      }
   }

   @ProtoFactory
   static JGroupsTopologyAwareAddress protoFactory(byte[] bytes) throws IOException {
      return (JGroupsTopologyAwareAddress) JGroupsAddress.protoFactory(bytes);
   }

   public JGroupsTopologyAwareAddress(ExtendedUUID address) {
      super(address);
   }

   @Override
   public String getSiteId() {
      return getString(SITE_KEY);
   }

   @Override
   public String getRackId() {
      return getString(RACK_KEY);
   }

   @Override
   public String getMachineId() {
      return getString(MACHINE_KEY);
   }

   public boolean matches(String siteId, String rackId, String machineId) {
      return checkComponent(SITE_KEY, siteId) &&
            checkComponent(RACK_KEY, rackId) &&
            checkComponent(MACHINE_KEY, machineId);
   }

   @Override
   public boolean isSameSite(TopologyAwareAddress addr) {
      if (addr instanceof JGroupsTopologyAwareAddress) {
         ExtendedUUID otherUUID = ((JGroupsTopologyAwareAddress) addr).topologyAddress();
         return checkComponent(SITE_KEY, otherUUID);
      }
      return checkComponent(SITE_KEY, addr.getSiteId());
   }

   @Override
   public boolean isSameRack(TopologyAwareAddress addr) {
      if (addr instanceof JGroupsTopologyAwareAddress) {
         ExtendedUUID otherUUID = ((JGroupsTopologyAwareAddress) addr).topologyAddress();
         return checkComponent(SITE_KEY, otherUUID) && checkComponent(RACK_KEY, otherUUID);
      }
      return checkComponent(SITE_KEY, addr.getSiteId()) && checkComponent(RACK_KEY, addr.getRackId());
   }

   @Override
   public boolean isSameMachine(TopologyAwareAddress addr) {
      if (addr instanceof JGroupsTopologyAwareAddress) {
         ExtendedUUID otherUUID = ((JGroupsTopologyAwareAddress) addr).topologyAddress();
         return checkComponent(SITE_KEY, otherUUID) && checkComponent(RACK_KEY, otherUUID) &&
               checkComponent(MACHINE_KEY, otherUUID);
      }
      return checkComponent(SITE_KEY, addr.getSiteId()) && checkComponent(RACK_KEY, addr.getRackId()) &&
            checkComponent(MACHINE_KEY, addr.getMachineId());
   }

   private boolean checkComponent(byte[] key, ExtendedUUID uuid) {
      return checkComponent(key, uuid.get(key));
   }

   private boolean checkComponent(byte[] key, String stringValue) {
      return checkComponent(key, Util.stringToBytes(stringValue));
   }

   private boolean checkComponent(byte[] key, byte[] expectedValue) {
      return Arrays.equals(getBytes(key), expectedValue);
   }

   private String getString(byte[] key) {
      return Util.bytesToString(getBytes(key));
   }

   private byte[] getBytes(byte[] key) {
      return topologyAddress().get(key);
   }

   private ExtendedUUID topologyAddress() {
      return (ExtendedUUID) address;
   }
}
