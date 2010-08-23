package org.infinispan.server.hotrod

import org.testng.annotations.Test
import org.infinispan.manager.DefaultCacheManager
import org.testng.Assert._

/**
 * Hot Rod server unit test.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodServerTest")
class HotRodServerTest {

   def testValidateProtocolServerNullProperties {
      val server = new HotRodServer
      try {
         server.start(null, new DefaultCacheManager)
         assertEquals(server.getHost, "127.0.0.1")
         assertEquals(server.getPort, 11311)
      } finally {
         server.stop
      }
   }

}