/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

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
      hash.value = 1;
      List<InetSocketAddress> list = list(a1, a2, a3, a4);
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

   public class DummyHash implements Hash {

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
   }

}
