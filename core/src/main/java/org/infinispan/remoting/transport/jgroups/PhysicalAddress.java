package org.infinispan.remoting.transport.jgroups;

import org.infinispan.remoting.transport.Address;

class PhysicalAddress implements Address {
   private final org.jgroups.Address address;

   PhysicalAddress(org.jgroups.Address address) {
      this.address = address;
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      PhysicalAddress that = (PhysicalAddress) o;
      return address.hashCode() == that.address.hashCode() && address.equals(that.address);
   }

   @Override
   public int hashCode() {
      return address.hashCode();
   }

   @Override
   public int compareTo(Address o) {
      return address.compareTo(((PhysicalAddress) o).address);
   }

   @Override
   public String toString() {
      return String.valueOf(address);
   }
}
