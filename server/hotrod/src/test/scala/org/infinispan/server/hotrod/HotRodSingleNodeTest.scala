package org.infinispan.server.hotrod

import org.infinispan.test.SingleCacheManagerTest
import test.HotRodClient
import org.infinispan.AdvancedCache
import test.HotRodTestingUtil._
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.core.test.ServerTestingUtil._
import org.testng.annotations.{Test, AfterClass}
import io.netty.channel.ChannelFuture

/**
 * Base test class for single node Hot Rod tests.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class HotRodSingleNodeTest extends SingleCacheManagerTest {
   val cacheName = "HotRodCache"
   protected var hotRodServer: HotRodServer = _
   private var hotRodClient: HotRodClient = _
   private var advancedCache: AdvancedCache[Array[Byte], Array[Byte]] = _
   private val hotRodJmxDomain = getClass.getSimpleName
   
   override def createCacheManager: EmbeddedCacheManager = {
      val cacheManager = createTestCacheManager
      advancedCache = cacheManager.getCache[Array[Byte], Array[Byte]](cacheName).getAdvancedCache
      cacheManager
   }

   @Test(enabled = false) // Disable explicitly to avoid TestNG thinking this is a test!!
   protected override def setup() {
      super.setup()
      hotRodServer = createStartHotRodServer(cacheManager)
      hotRodClient = connectClient
   }

   protected def createTestCacheManager: EmbeddedCacheManager =
      TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration())

   protected def createStartHotRodServer(cacheManager: EmbeddedCacheManager) = startHotRodServer(cacheManager)

   @AfterClass(alwaysRun = true)
   override def destroyAfterClass() {
      log.debug("Test finished, close cache, client and Hot Rod server")
      super.destroyAfterClass()
      shutdownClient
      killServer(hotRodServer)
   }

   protected def server = hotRodServer

   protected def client = hotRodClient

   protected def jmxDomain = hotRodJmxDomain

   protected def shutdownClient: ChannelFuture = killClient(hotRodClient)

   protected def connectClient: HotRodClient = new HotRodClient("127.0.0.1", hotRodServer.getPort, cacheName, 60, 10)
}
