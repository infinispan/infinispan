package org.infinispan.remoting.transport;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.jgroups.AddressCache;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.NameCache;
import org.jgroups.util.Util;

/**
 * An encapsulation of a JGroups {@link ExtendedUUID} with a {@link NodeVersion}, site id, rack id, and a machine id.
 *
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.JGROUPS_ADDRESS)
public class Address implements Comparable<Address> {
   private static final byte[] VERSION_KEY = new byte[]{'v'};
   private static final byte[] SITE_KEY = new byte[]{'s'};
   private static final byte[] RACK_KEY = new byte[]{'r'};
   private static final byte[] MACHINE_KEY = new byte[]{'m'};

   public static final Address LOCAL = random();

   @ProtoField(value = 1, defaultValue = "0")
   final long mostSignificantBits;
   @ProtoField(value = 2, defaultValue = "0")
   final long leastSignificantBits;
   @ProtoField(3)
   final NodeVersion version;
   @ProtoField(4)
   final String siteId;
   @ProtoField(5)
   final String rackId;
   @ProtoField(6)
   final String machineId;
   private transient String cachedName;

   public static Address random() {
      return random(null, null, null, null);
   }

   public static Address random(String name) {
      return random(name, null, null, null);
   }

   public static Address random(String name, String siteId, String rackId, String machineId) {
      var uuid = randomUUID(name);
      return new Address(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(), NodeVersion.INSTANCE, siteId, rackId, machineId);
   }

   public static ExtendedUUID randomUUID() {
      return randomUUID(null, null, null, null);
   }

   public static ExtendedUUID randomUUID(String name, String siteId, String rackId, String machineId) {
      return randomUUID(name, NodeVersion.INSTANCE, siteId, rackId, machineId);
   }

   public static ExtendedUUID randomUUID(String name, NodeVersion version, String siteId, String rackId, String machineId) {
      ExtendedUUID uuid = randomUUID(name);
      addId(uuid, VERSION_KEY, version.toString());
      addId(uuid, SITE_KEY, siteId);
      addId(uuid, RACK_KEY, rackId);
      addId(uuid, MACHINE_KEY, machineId);
      return uuid;
   }

   private static ExtendedUUID randomUUID(String name) {
      ExtendedUUID uuid = ExtendedUUID.randomUUID(name);
      if (name != null) {
         NameCache.add(uuid, name);
      }
      return uuid;
   }

   public static ExtendedUUID toExtendedUUID(Address address) {
      var uuid = new ExtendedUUID(address.getMostSignificantBits(), address.getLeastSignificantBits());
      addId(uuid, VERSION_KEY, address.getVersion().toString());
      addId(uuid, SITE_KEY, address.siteId);
      addId(uuid, RACK_KEY, address.rackId);
      addId(uuid, MACHINE_KEY, address.machineId);
      return uuid;
   }

   public static Address fromExtendedUUID(ExtendedUUID address) {
      return new Address(
            address.getMostSignificantBits(),
            address.getLeastSignificantBits(),
            NodeVersion.from(Util.bytesToString(address.get(VERSION_KEY))),
            Util.bytesToString(address.get(SITE_KEY)),
            Util.bytesToString(address.get(RACK_KEY)),
            Util.bytesToString(address.get(MACHINE_KEY))
      );
   }

   private static void addId(ExtendedUUID uuid, byte[] key, String stringValue) {
      if (stringValue != null) {
         uuid.put(key, Util.stringToBytes(stringValue));
      }
   }

   private Address(long mostSignificantBits, long leastSignificantBits, NodeVersion version, String siteId,
                   String rackId, String machineId) {
      this.mostSignificantBits = mostSignificantBits;
      this.leastSignificantBits = leastSignificantBits;
      this.version = version;
      this.siteId = siteId;
      this.rackId = rackId;
      this.machineId = machineId;
   }

   @ProtoFactory
   public static Address protoFactory(long mostSignificantBits, long leastSignificantBits, NodeVersion version,
                                      String siteId, String rackId, String machineId) {
      var existing = AddressCache.getIfPresent(mostSignificantBits, leastSignificantBits);
      if (existing != null) {
         return existing;
      }
      return new Address(mostSignificantBits, leastSignificantBits, version, siteId, rackId, machineId);
   }

   public long getLeastSignificantBits() {
      return leastSignificantBits;
   }

   public long getMostSignificantBits() {
      return mostSignificantBits;
   }

   public NodeVersion getVersion() {
      return version;
   }

   public String getSiteId() {
      return siteId;
   }

   public String getRackId() {
      return rackId;
   }

   public String getMachineId() {
      return machineId;
   }

   public boolean matches(String siteId, String rackId, String machineId) {
      return Objects.equals(siteId, this.siteId) &&
            Objects.equals(rackId, this.rackId) &&
            Objects.equals(machineId, this.machineId);
   }

   public boolean isSameSite(Address other) {
      return Objects.equals(siteId, other.getSiteId());
   }

   public boolean isSameRack(Address other) {
      return isSameSite(other) && Objects.equals(rackId, other.getRackId());
   }

   public boolean isSameMachine(Address other) {
      return isSameRack(other) && Objects.equals(machineId, other.getMachineId());
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;

      Address that = (Address) o;
      return mostSignificantBits == that.mostSignificantBits &&
            leastSignificantBits == that.leastSignificantBits;
   }

   @Override
   public int hashCode() {
      int result = Long.hashCode(mostSignificantBits);
      result = 31 * result + Long.hashCode(leastSignificantBits);
      return result;
   }

   @Override
   public String toString() {
      if (cachedName != null) {
         return cachedName;
      }
      var name = NameCache.get(new org.jgroups.util.UUID(getMostSignificantBits(), getLeastSignificantBits()));
      return name != null ? (cachedName = name) : toStringLong();
   }

   @Override
   public int compareTo(Address other) {
      int mostSigBits = Long.compare(this.mostSignificantBits, other.mostSignificantBits);
      return mostSigBits != 0 ? mostSigBits : Long.compare(this.leastSignificantBits, other.mostSignificantBits);
   }

   public String toStringLong() {
      return (digits(mostSignificantBits >> 32, 8) + "-" +
            digits(mostSignificantBits >> 16, 4) + "-" +
            digits(mostSignificantBits, 4) + "-" +
            digits(leastSignificantBits >> 48, 4) + "-" +
            digits(leastSignificantBits, 12));
   }

   private static String digits(long val, int digits) {
      long hi = 1L << (digits * 4);
      return Long.toHexString(hi | (val & (hi - 1))).substring(1);
   }
}
