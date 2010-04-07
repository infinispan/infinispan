package org.infinispan.server.hotrod

import test.HotRodTestingUtil._
import org.testng.annotations.Test
import org.infinispan.test.fwk.TestCacheManagerFactory
import java.lang.reflect.Method
import org.testng.Assert._
import org.infinispan.manager.CacheManager

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.FunctionalTest")
class HotRodStatsTest extends HotRodSingleNodeTest {

   override def createTestCacheManager: CacheManager = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(jmxDomain)

   def testStats(m: Method) {
      var s = client.stats
      assertTrue(s.get("timeSinceStart") != 0)
      assertEquals(s.get("currentNumberOfEntries").get, "0")
      assertEquals(s.get("totalNumberOfEntries").get, "0")
      assertEquals(s.get("stores").get, "0")
      assertEquals(s.get("retrievals").get, "0")
      assertEquals(s.get("hits").get, "0")
      assertEquals(s.get("misses").get, "0")
      assertEquals(s.get("removeHits").get, "0")
      assertEquals(s.get("removeMisses").get, "0")

      client.assertPut(m)
      s = client.stats
      assertEquals(s.get("currentNumberOfEntries").get, "1")
      assertEquals(s.get("totalNumberOfEntries").get, "1")
      assertEquals(s.get("stores").get, "1")
      val (getSt, actual) = client.assertGet(m)
      assertSuccess(getSt, v(m), actual)
      s = client.stats
      assertEquals(s.get("hits").get, "1")
      assertEquals(s.get("misses").get, "0")
      assertEquals(s.get("retrievals").get, "1")
   }

}