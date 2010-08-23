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