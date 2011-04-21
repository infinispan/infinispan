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
package org.infinispan.server.core

import org.testng.annotations.Test
import org.infinispan.remoting.transport.Address
import org.testng.Assert._
import org.infinispan.server.core.VersionGenerator._

/**
 * Version generator test
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.core.VersionGeneratorTest")
class VersionGeneratorTest {

   def testGenerateVersion {
      resetCounter
      val addr1 = new TestAddress(1)
      val addr2 = new TestAddress(2)
      val addr3 = new TestAddress(1)
      val members = List(addr1, addr2, addr3)
      RankCalculator.calculateRank(addr2, members, 1)
      assertEquals(newVersion(true), 0x1000200000001L)
      assertEquals(newVersion(true), 0x1000200000002L)
      assertEquals(newVersion(true), 0x1000200000003L)
   }

}

class TestAddress(val addressNum: Int) extends Address {
   override def equals(o: Any): Boolean = {
      o match {
         case ta: TestAddress => ta.addressNum == addressNum
         case _ => false
      }
   }
   override def hashCode = addressNum
   override def toString = "TestAddress#" + addressNum
}