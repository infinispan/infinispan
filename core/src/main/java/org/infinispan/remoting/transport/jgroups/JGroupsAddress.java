package org.infinispan.remoting.transport.jgroups;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
 * An encapsulation of a JGroups Address
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.JGROUPS_ADDRESS)
public class JGroupsAddress implements Address {

   protected final org.jgroups.Address address;
   private final int hashCode;
   private volatile byte[] bytes;

   public JGroupsAddress(final org.jgroups.Address address) {
      if (address == null)
         throw new IllegalArgumentException("Address shall not be null");
      this.address = address;
      this.hashCode = address.hashCode();
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
