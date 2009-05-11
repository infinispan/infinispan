package org.infinispan.remoting.transport.jgroups;

import org.infinispan.remoting.transport.Address;

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
}
