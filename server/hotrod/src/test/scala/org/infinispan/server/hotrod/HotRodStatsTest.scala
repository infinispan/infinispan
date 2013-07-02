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
@Test(groups = Array("functional"), testName = "server.hotrod.FunctionalTest")
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
      assertTrue(s.get("timeSinceStart") != "0")
      assertEquals(s.get("currentNumberOfEntries").get, "0")
      assertEquals(s.get("totalNumberOfEntries").get, "0")
      assertEquals(s.get("stores").get, "0")
      assertEquals(s.get("retrievals").get, "0")
      assertEquals(s.get("hits").get, "0")
      assertEquals(s.get("misses").get, "0")
      assertEquals(s.get("removeHits").get, "0")
      assertEquals(s.get("removeMisses").get, "0")
      bytesRead = assertHigherBytes(bytesRead, s.get("totalBytesRead"))
      // At time of request, no data had been written yet
      assertEquals(s.get("totalBytesWritten").get, "0")

      client.assertPut(m)
      s = client.stats
      assertEquals(s.get("currentNumberOfEntries").get, "1")
      assertEquals(s.get("totalNumberOfEntries").get, "1")
      assertEquals(s.get("stores").get, "1")
      bytesRead = assertHigherBytes(bytesRead, s.get("totalBytesRead"))
      bytesWritten = assertHigherBytes(bytesWritten, s.get("totalBytesWritten"))

      assertTrue(s.get("totalBytesRead") != "0")
      assertTrue(s.get("totalBytesWritten") != "0")

      assertSuccess(client.assertGet(m), v(m))
      s = client.stats
      assertEquals(s.get("hits").get, "1")
      assertEquals(s.get("misses").get, "0")
      assertEquals(s.get("retrievals").get, "1")
      bytesRead = assertHigherBytes(bytesRead, s.get("totalBytesRead"))
      bytesWritten = assertHigherBytes(bytesWritten, s.get("totalBytesWritten"))

      client.clear

      s = client.stats
      assertEquals(s.get("totalNumberOfEntries").get, "1")
      assertEquals(s.get("currentNumberOfEntries").get, "0")
   }

   private def assertHigherBytes(currentBytesRead: Int, bytesStr: Option[String]): Int = {
      val bytesRead = bytesStr.get.toInt
      assertTrue(bytesRead > currentBytesRead)
      bytesRead
   }
}