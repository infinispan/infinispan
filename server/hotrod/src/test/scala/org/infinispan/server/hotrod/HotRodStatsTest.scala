package org.infinispan.server.hotrod

import test.{Utils, Client}
import org.infinispan.test.SingleCacheManagerTest
import org.testng.annotations.{AfterClass, Test}
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.server.core.CacheValue
import org.infinispan.AdvancedCache
import org.jboss.netty.channel.Channel
import java.lang.reflect.Method
import org.testng.Assert._
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.manager.{DefaultCacheManager, CacheManager}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */
@Test(groups = Array("functional"), testName = "server.hotrod.FunctionalTest")
class HotRodStatsTest extends SingleCacheManagerTest with Utils with Client {
   private val cacheName = "hotrod-cache"
   private var server: HotRodServer = _
   private var ch: Channel = _
   private var advancedCache: AdvancedCache[CacheKey, CacheValue] = _
   private var jmxDomain = classOf[HotRodStatsTest].getSimpleName

   override def createCacheManager: CacheManager = {
      val cacheManager = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(jmxDomain)
      advancedCache = cacheManager.getCache[CacheKey, CacheValue](cacheName).getAdvancedCache
      server = startHotRodServer(cacheManager)
      ch = connect("127.0.0.1", server.getPort)
      cacheManager
   }

   @AfterClass(alwaysRun = true)
   override def destroyAfterClass {
      super.destroyAfterClass
      log.debug("Test finished, close client and Hot Rod server", null)
      ch.disconnect
      server.stop
   }

   def testStats(m: Method) {
      var s = stats(ch, cacheName)
      assertTrue(s.get("timeSinceStart") != 0)
      assertEquals(s.get("currentNumberOfEntries").get, "0")
      assertEquals(s.get("totalNumberOfEntries").get, "0")
      assertEquals(s.get("stores").get, "0")
      assertEquals(s.get("retrievals").get, "0")
      assertEquals(s.get("hits").get, "0")
      assertEquals(s.get("misses").get, "0")
      assertEquals(s.get("removeHits").get, "0")
      assertEquals(s.get("removeMisses").get, "0")
      assertEquals(s.get("evictions").get, "0")

      doPut(m)
      s = stats(ch, cacheName)
      assertEquals(s.get("currentNumberOfEntries").get, "1")
      assertEquals(s.get("totalNumberOfEntries").get, "1")
      assertEquals(s.get("stores").get, "1")
      val (getSt, actual) = doGet(m)
      assertSuccess(getSt, v(m), actual)
      s = stats(ch, cacheName)
      assertEquals(s.get("hits").get, "1")
      assertEquals(s.get("misses").get, "0")
      assertEquals(s.get("retrievals").get, "1")
   }

   // TODO: shared this private between tests by making client trait and object instead
   private def doPut(m: Method) {
      doPutWithLifespanMaxIdle(m, 0, 0)
   }

   private def doPutWithLifespanMaxIdle(m: Method, lifespan: Int, maxIdle: Int) {
      val status = put(ch, cacheName, k(m) , lifespan, maxIdle, v(m))
      assertStatus(status, Success)
   }

   private def doGet(m: Method): (OperationStatus, Array[Byte]) = {
      doGet(m, 0)
   }

   private def doGet(m: Method, flags: Int): (OperationStatus, Array[Byte]) = {
      get(ch, cacheName, k(m), flags)
   }
   
}