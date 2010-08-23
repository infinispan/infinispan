package org.infinispan.remoting.transport.jgroups;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.remoting.transport.Address;

/**
 * An encapsulation of a JGroups Address
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = JGroupsAddress.Externalizer.class, id = Ids.JGROUPS_ADDRESS)
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
   
   public static class Externalizer implements org.infinispan.marshall.Externalizer {
      public void writeObject(ObjectOutput output, Object subject) throws IOException {
         JGroupsAddress address = (JGroupsAddress) subject;
         output.writeObject(address.address);
      }

      public Object readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         JGroupsAddress address = new JGroupsAddress();
         address.address = (org.jgroups.Address) unmarshaller.readObject();
         return address;
      }
   }
}
