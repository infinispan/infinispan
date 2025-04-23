package org.infinispan.remoting.transport.jgroups;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
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
@ProtoTypeId(ProtoStreamTypeIds.JGROUPS_ADDRESS)
public class JGroupsAddress implements TopologyAwareAddress {
   private static final byte[] SITE_KEY = Util.stringToBytes("site-id");
   private static final byte[] RACK_KEY = Util.stringToBytes("rack-id");
   private static final byte[] MACHINE_KEY = Util.stringToBytes("machine-id");

   public static final JGroupsAddress LOCAL = new JGroupsAddress(ExtendedUUID.randomUUID());

   protected final org.jgroups.Address address;
   private final int hashCode;
   private volatile byte[] bytes;

   public static JGroupsAddress random() {
      var uuid = randomUUID(null, null, null, null);
      return new JGroupsAddress(uuid);
   }

   public static JGroupsAddress random(String name) {
      var uuid = randomUUID(name, null, null, null);
      return new JGroupsAddress(uuid);
   }

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
   static JGroupsAddress protoFactory(byte[] bytes) throws IOException {
      try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
         // Note: Use org.jgroups.Address, not the concrete UUID class.
         // Otherwise applications that only use local caches would have to bundle the JGroups jar,
         // because the verifier needs to check the arguments of fromJGroupsAddress
         // even if this method is never called.
         org.jgroups.Address address = org.jgroups.util.Util.readAddress(in);
         return (JGroupsAddress) JGroupsAddressCache.fromJGroupsAddress(address);
      } catch (ClassNotFoundException e) {
         throw new MarshallingException(e);
      }
   }

   public JGroupsAddress(ExtendedUUID address) {
      if (address == null)
         throw new IllegalArgumentException("Address shall not be null");
      this.address = address;
      this.hashCode = address.hashCode();
   }

   @ProtoField(1)
   byte[] getBytes() throws IOException {
      if (bytes == null) {
         try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
              DataOutputStream out = new DataOutputStream(baos)) {
            org.jgroups.util.Util.writeAddress(address, out);
            bytes = baos.toByteArray();
         }
      }
      return bytes;
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
      if (addr instanceof JGroupsAddress) {
         ExtendedUUID otherUUID = ((JGroupsAddress) addr).topologyAddress();
         return checkComponent(SITE_KEY, otherUUID);
      }
      return checkComponent(SITE_KEY, addr.getSiteId());
   }

   @Override
   public boolean isSameRack(TopologyAwareAddress addr) {
      if (addr instanceof JGroupsAddress) {
         ExtendedUUID otherUUID = ((JGroupsAddress) addr).topologyAddress();
         return checkComponent(SITE_KEY, otherUUID) && checkComponent(RACK_KEY, otherUUID);
      }
      return checkComponent(SITE_KEY, addr.getSiteId()) && checkComponent(RACK_KEY, addr.getRackId());
   }

   @Override
   public boolean isSameMachine(TopologyAwareAddress addr) {
      if (addr instanceof JGroupsAddress) {
         ExtendedUUID otherUUID = ((JGroupsAddress) addr).topologyAddress();
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

   @Override
   public boolean equals(final Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      JGroupsAddress that = (JGroupsAddress) o;

      return hashCode == that.hashCode && address.equals(that.address);
   }

   @Override
   public int hashCode() {
      return hashCode;
   }

   @Override
   public String toString() {
      return String.valueOf(address);
   }

   public org.jgroups.Address getJGroupsAddress() {
      return address;
   }

   @Override
   public int compareTo(Address o) {
      JGroupsAddress oa = (JGroupsAddress) o;
      return address.compareTo(oa.address);
   }
}
