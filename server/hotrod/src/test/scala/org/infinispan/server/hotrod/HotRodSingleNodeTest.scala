package org.infinispan.server.hotrod

import org.infinispan.test.SingleCacheManagerTest
import org.infinispan.server.core.CacheValue
import test.HotRodClient
import org.infinispan.AdvancedCache
import org.infinispan.manager.CacheManager
import org.infinispan.test.fwk.TestCacheManagerFactory
import test.HotRodTestingUtil._
import org.testng.annotations.AfterClass
import org.jboss.netty.logging.{InternalLoggerFactory, Log4JLoggerFactory}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since // TODO
 */
abstract class HotRodSingleNodeTest extends SingleCacheManagerTest {
   val cacheName = "hotrod-cache"
   private var server: HotRodServer = _
   private var client: HotRodClient = _
   private var advancedCache: AdvancedCache[CacheKey, CacheValue] = _
   private var jmxDomain = getClass.getSimpleName
   
   override def createCacheManager: CacheManager = {
      val cacheManager = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(jmxDomain)
      advancedCache = cacheManager.getCache[CacheKey, CacheValue](cacheName).getAdvancedCache
      server = startHotRodServer(cacheManager)
      client = new HotRodClient("127.0.0.1", server.getPort, cacheName)
      cacheManager
   }

   @AfterClass(alwaysRun = true)
   override def destroyAfterClass {
      log.debug("Test finished, close cache, client and Hot Rod server", null)
      super.destroyAfterClass
      client.stop
      server.stop
   }

   def getServer = server

   def getClient = client
}