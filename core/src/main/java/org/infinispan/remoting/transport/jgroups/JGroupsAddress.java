package org.infinispan.remoting.transport.jgroups;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.marshall.InstanceReusingAdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;

/**
 * An encapsulation of a JGroups Address
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class JGroupsAddress implements Address {

   protected final org.jgroups.Address address;
   private final int hashCode;

   public JGroupsAddress(final org.jgroups.Address address) {
      if (address == null)
         throw new IllegalArgumentException("Address shall not be null");
      this.address = address;
      this.hashCode = address.hashCode();
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

   public static final class Externalizer extends InstanceReusingAdvancedExternalizer<JGroupsAddress> {

      public Externalizer() {
         super(false);
      }

      @Override
      public void doWriteObject(ObjectOutput output, JGroupsAddress address) throws IOException {
         try {
            org.jgroups.util.Util.writeAddress(address.address, output);
         } catch (Exception e) {
            throw new IOException(e);
         }
      }

      @Override
      public JGroupsAddress doReadObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         try {
            org.jgroups.Address address = org.jgroups.util.Util.readAddress(unmarshaller);
            return (JGroupsAddress) JGroupsAddressCache.fromJGroupsAddress(address);
         } catch (Exception e) {
            throw new IOException(e);
         }
      }

      @Override
      public Integer getId() {
         return Ids.JGROUPS_ADDRESS;
      }

      @Override
      public Set<Class<? extends JGroupsAddress>> getTypeClasses() {
         return Util.<Class<? extends JGroupsAddress>>asSet(JGroupsAddress.class);
      }
   }
}
