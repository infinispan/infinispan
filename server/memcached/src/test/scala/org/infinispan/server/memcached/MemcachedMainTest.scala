package org.infinispan.server.memcached

import org.testng.annotations.Test
import org.infinispan.server.core.Main
import test.MemcachedTestingUtil
import org.testng.Assert._

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(groups = Array("functional"), testName = "server.memcached.MemcachedMainTest")
class MemcachedMainTest extends MemcachedTestingUtil {

   def testMainNoConfigExposesStatistics = {
      Main.boot(Array("-r", "memcached", "-p", "23345"))

      try {
         val memcachedClient = createMemcachedClient(60000, 23345)
         val allStats = memcachedClient.getStats()
         assertEquals(allStats.size(), 1)
         val stats = allStats.values.iterator.next
         assertEquals(stats.get("cmd_set"), "0")
      } finally {
         Main.getServer.stop
         Main.getCacheManager.stop
      }
   }
   
}