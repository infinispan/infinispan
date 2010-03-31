package org.infinispan.server.core

import org.testng.annotations.Test
import org.infinispan.remoting.transport.Address
import org.testng.Assert._
import org.infinispan.server.core.VersionGenerator._

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.core.VersionGeneratorTest")
class VersionGeneratorTest {

   def testGenerateVersion {
      val addr1 = new TestAddress(1)
      val addr2 = new TestAddress(2)
      val addr3 = new TestAddress(1)
      val members = List(addr1, addr2, addr3)
      assertEquals(newVersion(Some(addr2), Some(members), 1), 0x1000200000001L)
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