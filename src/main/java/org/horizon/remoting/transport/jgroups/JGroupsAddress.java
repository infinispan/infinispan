package org.horizon.remoting.transport.jgroups;

import org.horizon.remoting.transport.Address;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * An encapsulation of a JGroups Address
 *
 * @author Manik Surtani
 * @since 1.0
 */
public class JGroupsAddress implements Address, Externalizable {
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

   public void writeExternal(ObjectOutput out) throws IOException {
      out.writeObject(address);
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      address = (org.jgroups.Address) in.readObject();
   }
}
