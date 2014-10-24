package org.infinispan.server.hotrod

import java.lang.reflect.Method
import java.util.Arrays
import test.HotRodTestingUtil._
import org.testng.Assert._
import org.infinispan.server.hotrod.OperationStatus._

import org.infinispan.server.hotrod.test.{TestResponseWithPrevious, HotRodClient}
import org.testng.annotations.Test

@Test(groups = Array("functional"), testName = "server.hotrod.HotRod1xFunctionalTest")
class HotRod1xFunctionalTest extends HotRodFunctionalTest {
   override protected def connectClient: HotRodClient =
      new HotRodClient("127.0.0.1", hotRodServer.getPort, cacheName, 60, 13)

   override def testSize(m: Method): Unit = {
      // Not supported
   }

   override protected def assertSuccessPrevious(resp: TestResponseWithPrevious, expected: Array[Byte]): Boolean = {
      if (expected == null) assertEquals(None, resp.previous)
      else assertTrue(java.util.Arrays.equals(expected, resp.previous.get))
      assertStatus(resp, Success)
   }

   override protected def assertNotExecutedPrevious(resp: TestResponseWithPrevious, expected: Array[Byte]): Boolean = {
      if (expected == null) assertEquals(None, resp.previous)
      else assertTrue(java.util.Arrays.equals(expected, resp.previous.get))
      assertStatus(resp, OperationNotExecuted)
   }

}
