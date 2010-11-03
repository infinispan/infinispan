package org.infinispan.distribution;

import org.infinispan.remoting.transport.Address;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class TestAddress implements Address {
   int addressNum;

   String name;

   public void setName(String name) {
      this.name = name;
   }

   public TestAddress(int addressNum) {
      this.addressNum = addressNum;
   }

   public int getAddressNum() {
      return addressNum;
   }

   public void setAddressNum(int addressNum) {
      this.addressNum = addressNum;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TestAddress that = (TestAddress) o;

      if (addressNum != that.addressNum) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return addressNum;
   }

   @Override
   public String toString() {
      return "TestAddress#"+addressNum + (name != null? (" " + name) : "");
   }

   public int compareTo(Object o) {
      return this.addressNum - ((TestAddress) o).addressNum;
   }
}
