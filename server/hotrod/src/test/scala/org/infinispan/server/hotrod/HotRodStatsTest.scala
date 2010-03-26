package org.infinispan.server.hotrod

import test.HotRodClient
import test.HotRodTestingUtil._
import org.infinispan.test.SingleCacheManagerTest
import org.testng.annotations.{AfterClass, Test}
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.server.core.CacheValue
import org.infinispan.AdvancedCache
import java.lang.reflect.Method
import org.testng.Assert._
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.manager.CacheManager

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.FunctionalTest")
class HotRodStatsTest extends SingleCacheManagerTest {
   private val cacheName = "hotrod-cache"
   private var server: HotRodServer = _
   private var client: HotRodClient = _
   private var advancedCache: AdvancedCache[CacheKey, CacheValue] = _
   private var jmxDomain = classOf[HotRodStatsTest].getSimpleName

   override def createCacheManager: CacheManager = {
      val cacheManager = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(jmxDomain)
      advancedCache = cacheManager.getCache[CacheKey, CacheValue](cacheName).getAdvancedCache
      server = startHotRodServer(cacheManager)
      client = new HotRodClient("127.0.0.1", server.getPort, cacheName)
      cacheManager
   }

   @AfterClass(alwaysRun = true)
   override def destroyAfterClass {
      super.destroyAfterClass
      log.debug("Test finished, close client and Hot Rod server", null)
      client.stop
      server.stop
   }

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