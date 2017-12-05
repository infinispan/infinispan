package org.infinispan.distribution;

import java.util.Objects;

import org.infinispan.remoting.transport.Address;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class TestAddress implements Address {
   private final int addressNum;

   private String name;

   public void setName(String name) {
      this.name = name;
   }

   public TestAddress(int addressNum) {
      this.addressNum = addressNum;
   }

   public TestAddress(int addressNum, String name) {
      this.addressNum = addressNum;
      this.name = name;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TestAddress that = (TestAddress) o;

      return addressNum == that.addressNum && Objects.equals(name, that.name);
   }

   @Override
   public int hashCode() {
      return addressNum;
   }

   @Override
   public String toString() {
      if (name != null) {
         return name + "#" + addressNum;
      } else
      return "TestAddress#" + addressNum;
   }

   @Override
   public int compareTo(Address o) {
      return this.addressNum - ((TestAddress) o).addressNum;
   }
}
