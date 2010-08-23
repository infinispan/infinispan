package org.infinispan.server.memcached

import org.testng.annotations.Test
import org.infinispan.manager.DefaultCacheManager
import org.testng.Assert._

/**
 * Memcached server unit test.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.memcached.MemcachedServerTest")
class MemcachedServerTest {

   def testValidateProtocolServerNullProperties {
      val server = new MemcachedServer
      try {
         server.start(null, new DefaultCacheManager)
         assertEquals(server.getHost, "127.0.0.1")
         assertEquals(server.getPort, 11211)
      } finally {
         server.stop
      }
   }

}