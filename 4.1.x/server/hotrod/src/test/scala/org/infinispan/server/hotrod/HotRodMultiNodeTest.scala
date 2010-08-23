package org.infinispan.server.hotrod

import org.infinispan.test.MultipleCacheManagersTest
import org.infinispan.config.Configuration
import org.testng.annotations.{AfterMethod, AfterClass, Test}
import test.HotRodClient
import test.HotRodTestingUtil._
import org.infinispan.manager.EmbeddedCacheManager

/**
 * Base test class for multi node or clustered Hot Rod tests.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class HotRodMultiNodeTest extends MultipleCacheManagersTest {
   private[this] var hotRodServers: List[HotRodServer] = List()
   private[this] var hotRodClients: List[HotRodClient] = List()

   @Test(enabled=false) // Disable explicitly to avoid TestNG thinking this is a test!!
   override def createCacheManagers {
      for (i <- 0 until 2) {
         val cm = addClusterEnabledCacheManager()
         cm.defineConfiguration(cacheName, createCacheConfig)
      }
      hotRodServers = hotRodServers ::: List(startTestHotRodServer(cacheManagers.get(0)))
      hotRodServers = hotRodServers ::: List(startTestHotRodServer(cacheManagers.get(1), hotRodServers.head.getPort + 50))
      hotRodServers.foreach {s =>
         hotRodClients = new HotRodClient("127.0.0.1", s.getPort, cacheName, 60) :: hotRodClients
      }
   }

   protected def startTestHotRodServer(cacheManager: EmbeddedCacheManager) = startHotRodServer(cacheManager)

   protected def startTestHotRodServer(cacheManager: EmbeddedCacheManager, port: Int) = startHotRodServer(cacheManager, port)

   @AfterClass(alwaysRun = true)
   override def destroy {
      try {
         log.debug("Test finished, close Hot Rod server", null)
         hotRodClients.foreach(_.stop)
         hotRodServers.foreach(_.stop)
      } finally {
         super.destroy // Stop the caches last so that at stoppage time topology cache can be updated properly
      }
   }

   @AfterMethod(alwaysRun=true)
   override def clearContent() {
      // Do not clear cache between methods so that topology cache does not get cleared
   }

   protected def servers = hotRodServers

   protected def clients = hotRodClients

   protected def cacheName: String

   protected def createCacheConfig: Configuration

}