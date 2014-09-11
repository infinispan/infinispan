package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV1;
import org.infinispan.commons.hash.Hash;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus
 * @since 5.0
 */
@Test(groups = "unit", testName = "client.hotrod.ConsistentHashV1Test")
public class ConsistentHashV1Test {

   private InetSocketAddress a1;
   private InetSocketAddress a2;
   private InetSocketAddress a3;
   private InetSocketAddress a4;
   private DummyHash hash;
   private ConsistentHashV1 v1;


   private void setUp(int numOwners) {
      a1 = new InetSocketAddress(1);
      a2 = new InetSocketAddress(2);
      a3 = new InetSocketAddress(3);
      a4 = new InetSocketAddress(4);
      LinkedHashMap<SocketAddress, Set<Integer>> map = new LinkedHashMap<SocketAddress, Set<Integer>>();
      map.put(a1, Collections.singleton(0));
      map.put(a2, Collections.singleton(1000));
      map.put(a3, Collections.singleton(2000));
      map.put(a4, Collections.singleton(3000));

      this.v1 = new ConsistentHashV1();
      this.v1.init(map, numOwners, 10000);
      hash = new DummyHash();
      this.v1.setHash(hash);
   }


   public void simpleTest() {
      setUp(1);
      hash.value = 0;
      assert v1.getServer(new byte[0]).equals(a1);
      hash.value = 1;
      assert v1.getServer(new byte[0]).equals(a2);
      hash.value = 1001;
      assert v1.getServer(new byte[0]).equals(a3);
      hash.value = 2001;
      assertEquals(v1.getServer(new byte[0]), a4);
      hash.value = 3001;
      assert v1.getServer(new byte[0]).equals(a1);
   }

   public void numOwners2Test() {
      setUp(2);
      hash.value = 0;
      assert list(a1, a2).contains(v1.getServer(new byte[0]));

      hash.value = 1;
      assert list(a2, a3).contains(v1.getServer(new byte[0]));

      hash.value = 1001;
      assert list(a3, a4).contains(v1.getServer(new byte[0]));

      hash.value = 2001;
      assert list(a4, a1).contains(v1.getServer(new byte[0]));

      hash.value = 3001;
      assert list(a1, a2).contains(v1.getServer(new byte[0]));
   }

   public void numOwners3Test() {
      setUp(3);
      hash.value = 0;
      assert list(a1, a2, a3).contains(v1.getServer(new byte[0]));

      hash.value = 1;
      assert list(a2, a3, a4).contains(v1.getServer(new byte[0]));

      hash.value = 1001;
      assert list(a3, a4, a1).contains(v1.getServer(new byte[0]));

      hash.value = 2001;
      assert list(a4, a1, a2).contains(v1.getServer(new byte[0]));

      hash.value = 3001;
      assert list(a1, a2, a3).contains(v1.getServer(new byte[0]));
   }

   //now a bit more extreme...
   public void numOwners4Test() {
      setUp(4);

      List<InetSocketAddress> list = list(a1, a2, a3, a4);

      hash.value = 0;
      assert list.contains(v1.getServer(new byte[0]));

      hash.value = 1;
      assert list.contains(v1.getServer(new byte[0]));

      hash.value = 1001;
      assert list.contains(v1.getServer(new byte[0]));

      hash.value = 2001;
      assert list.contains(v1.getServer(new byte[0]));

      hash.value = 3001;
      assert list.contains(v1.getServer(new byte[0]));
   }

   private List<InetSocketAddress> list(InetSocketAddress... a) {
      return Arrays.asList(a);
   }


   public void testCorrectHash() {
      hash.value = 1;
      v1.getServer(new byte[0]);
   }

   public static class DummyHash implements Hash {

      public int value;

      @Override
      public int hash(byte[] payload) {
         return value;
      }

      @Override
      public int hash(int hashcode) {
         return value;
      }

      @Override
      public int hash(Object o) {
         return value;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         DummyHash dummyHash = (DummyHash) o;

         if (value != dummyHash.value) return false;

         return true;
      }

      @Override
      public int hashCode() {
         return value;
      }
   }

}
