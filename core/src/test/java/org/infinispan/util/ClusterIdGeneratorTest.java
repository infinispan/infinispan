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

package org.infinispan.util;

import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "util.ClusterIdGeneratorTest")
public class ClusterIdGeneratorTest {

   public void testGenerateVersion() {
      ClusterIdGenerator vg = new ClusterIdGenerator(null, null);
      vg.resetCounter();
      TestAddress addr1 = new TestAddress(1);
      TestAddress addr2 = new TestAddress(2);
      TestAddress addr3 = new TestAddress(1);
      List<Address> members = Arrays.asList((Address)addr1, addr2, addr3);
      vg.calculateRank(addr2, members, 1);


      assertEquals(vg.newVersion(true), (Object)0x1000200000001L);
      assertEquals(vg.newVersion(true), (Object)0x1000200000002L);
      assertEquals(vg.newVersion(true), (Object)0x1000200000003L);
   }
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
}