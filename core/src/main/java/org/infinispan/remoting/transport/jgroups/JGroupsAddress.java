package org.infinispan.remoting.transport.jgroups;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

/**
 * An encapsulation of a JGroups Address
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class JGroupsAddress implements Address {
   org.jgroups.Address address;

   public JGroupsAddress() {
   }

   public JGroupsAddress(org.jgroups.Address address) {
      this.address = address;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      JGroupsAddress that = (JGroupsAddress) o;

      if (address != null ? !address.equals(that.address) : that.address != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return address != null ? address.hashCode() : 0;
   }

   @Override
   public String toString() {
      if (address != null) {
         return address.toString();
      } else {
         return "null";
      }
   }

   public org.jgroups.Address getJGroupsAddress() {
      return address;
   }

   public void setJGroupsAddress(org.jgroups.Address address) {
      this.address = address;
   }

   public static class Externalizer extends AbstractExternalizer<JGroupsAddress> {
      @Override
      public void writeObject(ObjectOutput output, JGroupsAddress address) throws IOException {
         output.writeObject(address.address);
      }

      @Override
      public JGroupsAddress readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         JGroupsAddress address = new JGroupsAddress();
         address.address = (org.jgroups.Address) unmarshaller.readObject();
         return address;
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
