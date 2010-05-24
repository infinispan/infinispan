package org.infinispan.server.hotrod

import org.infinispan.test.SingleCacheManagerTest
import org.infinispan.server.core.CacheValue
import test.HotRodClient
import org.infinispan.AdvancedCache
import test.HotRodTestingUtil._
import org.testng.annotations.AfterClass
import org.jboss.netty.channel.ChannelFuture
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.manager.EmbeddedCacheManager

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class HotRodSingleNodeTest extends SingleCacheManagerTest {
   val cacheName = "HotRodCache"
   private var hotRodServer: HotRodServer = _
   private var hotRodClient: HotRodClient = _
   private var advancedCache: AdvancedCache[CacheKey, CacheValue] = _
   private var hotRodJmxDomain = getClass.getSimpleName
   
   override def createCacheManager: EmbeddedCacheManager = {
      val cacheManager = createTestCacheManager
      cacheManager.defineConfiguration(cacheName, cacheManager.getDefaultConfiguration)
      advancedCache = cacheManager.getCache[CacheKey, CacheValue](cacheName).getAdvancedCache
      hotRodServer = createStartHotRodServer(cacheManager)
      hotRodClient = connectClient
      cacheManager
   }

   protected def createTestCacheManager: EmbeddedCacheManager = TestCacheManagerFactory.createLocalCacheManager(true)

   protected def createStartHotRodServer(cacheManager: EmbeddedCacheManager) = startHotRodServer(cacheManager)

   @AfterClass(alwaysRun = true)
   override def destroyAfterClass {
      log.debug("Test finished, close cache, client and Hot Rod server", null)
      super.destroyAfterClass
      shutdownClient
      hotRodServer.stop
   }

   protected def server = hotRodServer

   protected def client = hotRodClient

   protected def jmxDomain = hotRodJmxDomain

   protected def shutdownClient: ChannelFuture = hotRodClient.stop

   protected def connectClient: HotRodClient = new HotRodClient("127.0.0.1", hotRodServer.getPort, cacheName, 60)
}