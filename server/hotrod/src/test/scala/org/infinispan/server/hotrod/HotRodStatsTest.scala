package org.infinispan.server.hotrod

import test.HotRodTestingUtil._
import org.testng.annotations.Test
import org.infinispan.test.fwk.TestCacheManagerFactory
import java.lang.reflect.Method
import org.testng.Assert._
import org.infinispan.manager.EmbeddedCacheManager

/**
 * Tests stats operation against a Hot Rod server.
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodStatsTest")
class HotRodStatsTest extends HotRodSingleNodeTest {

   override def createTestCacheManager: EmbeddedCacheManager = {
      val cfg = hotRodCacheConfiguration()
      cfg.jmxStatistics().enable()
      TestCacheManagerFactory.createClusteredCacheManagerEnforceJmxDomain(jmxDomain, cfg)
   }

   def testStats(m: Method) {
      var bytesRead = 0
      var bytesWritten = 0

      var s = client.stats
//      assertTrue(s.get("timeSinceStart") != "0")
      assertEquals(s.get("currentNumberOfEntries"), "0")
      assertEquals(s.get("totalNumberOfEntries"), "0")
      assertEquals(s.get("stores"), "0")
      assertEquals(s.get("retrievals"), "0")
      assertEquals(s.get("hits"), "0")
      assertEquals(s.get("misses"), "0")
      assertEquals(s.get("removeHits"), "0")
      assertEquals(s.get("removeMisses"), "0")
      bytesRead = assertHigherBytes(bytesRead, s.get("totalBytesRead"))
      // At time of request, no data had been written yet
      assertEquals(s.get("totalBytesWritten"), "0")

      client.assertPut(m)
      s = client.stats
      assertEquals(s.get("currentNumberOfEntries"), "1")
      assertEquals(s.get("totalNumberOfEntries"), "1")
      assertEquals(s.get("stores"), "1")
      bytesRead = assertHigherBytes(bytesRead, s.get("totalBytesRead"))
      bytesWritten = assertHigherBytes(bytesWritten, s.get("totalBytesWritten"))

      assertTrue(s.get("totalBytesRead") != "0")
      assertTrue(s.get("totalBytesWritten") != "0")

      assertSuccess(client.assertGet(m), v(m))
      s = client.stats
      assertEquals(s.get("hits"), "1")
      assertEquals(s.get("misses"), "0")
      assertEquals(s.get("retrievals"), "1")
      bytesRead = assertHigherBytes(bytesRead, s.get("totalBytesRead"))
      bytesWritten = assertHigherBytes(bytesWritten, s.get("totalBytesWritten"))

      client.clear

      s = client.stats
      assertEquals(s.get("totalNumberOfEntries"), "1")
      assertEquals(s.get("currentNumberOfEntries"), "0")
   }

   private def assertHigherBytes(currentBytesRead: Int, bytesStr: String): Int = {
      val bytesRead = bytesStr.toInt
      assertTrue(bytesRead > currentBytesRead)
      bytesRead
   }
}