package org.infinispan.distribution;

import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

@Test(groups = "unit", testName = "distribution.DefaultConsistentHashTest", enabled = true)
public class DefaultConsistentHashTest extends AbstractInfinispanTest {

   List<Address> servers;
   ConsistentHash ch;

   @BeforeTest
   public void setUp() {
      servers = new LinkedList<Address>();
      int numServers = 5;
      for (int i = 0; i < numServers; i++) {
         servers.add(new TestAddress(i));
      }

      ch = BaseDistFunctionalTest.createNewConsistentHash(servers);
   }

   @AfterTest
   public void tearDown() {
      servers = null;
      ch = null;
   }

   public void testSimpleHashing() {
      Object o = new Object();
      List<Address> l1 = ch.locate(o, 2);
      List<Address> l2 = ch.locate(o, 2);

      assert l1.size() == 2;
      assert l1.equals(l2);
      assert l1 != l2;

      Object o2 = new Object() {
         @Override
         public int hashCode() {
            return 4567890;
         }
      };

      Object o3 = new Object() {
         @Override
         public int hashCode() {
            return 4567890;
         }
      };

      assert o2 != o3;
      assert !o2.equals(o3);
      assert ch.locate(o2, 4).equals(ch.locate(o3, 4));
   }

   public void testMultipleKeys() {
      Object k1 = "key1", k2 = "key2", k3 = "key3";
      Collection<Object> keys = Arrays.asList(k1, k2, k3);
      Map<Object, List<Address>> locations = ch.locateAll(keys, 3);

      assert locations.size() == 3;
      for (Object k : keys) {
         assert locations.containsKey(k);
         assert locations.get(k).size() == 3;
      }
   }

   public void testDistances() {
      Address a1 = new TestAddress(1000);
      Address a2 = new TestAddress(2000);
      Address a3 = new TestAddress(3000);
      Address a4 = new TestAddress(4000);

      ch = BaseDistFunctionalTest.createNewConsistentHash(Arrays.asList(a1, a2, a3, a4));

      // the CH may reorder the addresses.  Get the new order.
      List<Address> adds = ch.getCaches();
      a1 = adds.get(0);
      a2 = adds.get(1);
      a3 = adds.get(2);
      a4 = adds.get(3);

      assert ch.getDistance(a1, a1) == 0;
      assert ch.getDistance(a1, a4) == 3;
      assert ch.getDistance(a1, a3) == 2;
      assert ch.getDistance(a3, a1) == 2;
      assert ch.getDistance(a1, a2) == 1;
      assert ch.getDistance(a2, a1) == 3;

      assert ch.isAdjacent(a1, a2);
      assert !ch.isAdjacent(a1, a3);
      assert ch.isAdjacent(a1, a4);
   }

   public void testNumHashedNodes() {
      Address a1 = new TestAddress(1000);
      Address a2 = new TestAddress(2000);
      Address a3 = new TestAddress(3000);
      Address a4 = new TestAddress(4000);

      ch = BaseDistFunctionalTest.createNewConsistentHash(Arrays.asList(a1, a2, a3, a4));

      String[] keys = new String[10000];
      Random r = new Random();
      for (int i=0; i<10000; i++) keys[i] = Integer.toHexString(r.nextInt());

      for (String key: keys) {
         List<Address> l = ch.locate(key, 2);
         assert l.size() == 2: "Did NOT find 2 owners for key ["+key+"] as expected!  Found " + l;
         assert ch.isAdjacent(l.get(0), l.get(1)) : "Nodes " + l + " should be adjacent!";
      }
   }
}

class TestAddress implements Address {
   int addressNum;

   TestAddress(int addressNum) {
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
      return "TestAddress#"+addressNum;
   }

   public int compareTo(Object o) {
      return this.addressNum - ((TestAddress) o).addressNum;
   }
}
