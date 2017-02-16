package org.infinispan.container.versioning;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

/**
 * Test numeric version generator logic
 */
@Test(groups = "functional", testName = "container.versioning.NumericVersionGeneratorTest")
public class NumericVersionGeneratorTest {

   public void testGenerateVersion() {
      NumericVersionGenerator vg = new NumericVersionGenerator().clustered(true);
      vg.resetCounter();
      TestAddress addr1 = new TestAddress(1);
      TestAddress addr2 = new TestAddress(2);
      TestAddress addr3 = new TestAddress(1);
      List<Address> members = Arrays.asList((Address)addr1, addr2, addr3);
      vg.calculateRank(addr2, members, 1);


      assertEquals(new NumericVersion(0x1000200000001L), vg.generateNew());
      assertEquals(new NumericVersion(0x1000200000002L), vg.generateNew());
      assertEquals(new NumericVersion(0x1000200000003L), vg.generateNew());
   }

   class TestAddress implements Address {
      int addressNum;

      TestAddress(int addressNum) {
         this.addressNum = addressNum;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         if (!super.equals(o)) return false;

         TestAddress that = (TestAddress) o;

         if (addressNum != that.addressNum) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = super.hashCode();
         result = 31 * result + addressNum;
         return result;
      }

      @Override
      public int compareTo(Address o) {
         TestAddress oa = (TestAddress) o;
         return addressNum - oa.addressNum;
      }
   }

}
