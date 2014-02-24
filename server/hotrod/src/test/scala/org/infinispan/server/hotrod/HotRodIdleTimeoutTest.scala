package org.infinispan.server.hotrod

import org.testng.annotations.Test
import java.lang.reflect.Method
import test.HotRodTestingUtil._
import org.testng.Assert._
import test.{HotRodClient, UniquePortThreadLocal}
import org.infinispan.manager.EmbeddedCacheManager

/**
 * Tests idle timeout logic in Hot Rod.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodIdleTimeoutTest")
class HotRodIdleTimeoutTest extends HotRodSingleNodeTest {

   override protected def createStartHotRodServer(cacheManager: EmbeddedCacheManager) =
      startHotRodServer(cacheManager, UniquePortThreadLocal.get.intValue, 5)

   override protected def connectClient = new HotRodClient("127.0.0.1", server.getPort, cacheName, 10, 20)

   def testSendPartialRequest(m: Method) {
      client.assertPut(m)
      val resp = client.executePartial(0xA0, 0x03, cacheName, k(m) , 0, 0, v(m), 0)
      assertNull(resp) // No response received within expected timeout.
      client.assertPutFail(m)
      shutdownClient
      
      val newClient = connectClient
      try {
         newClient.assertPut(m)
      } finally {
         shutdownClient
      }
   }

}