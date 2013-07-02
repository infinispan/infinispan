package org.infinispan.server.hotrod

import org.infinispan.test.MultipleCacheManagersTest
import org.testng.annotations.{AfterMethod, AfterClass, Test}
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.configuration.cache.CacheMode
import org.infinispan.commons.equivalence.ByteArrayEquivalence

// Do not remove, otherwise getDefaultClusteredConfig is not found
import org.infinispan.test.AbstractCacheTest._
import test.{UniquePortThreadLocal, HotRodClient}
import scala.concurrent.ops._
import test.HotRodTestingUtil._
import org.infinispan.server.core.test.ServerTestingUtil._

/**
 * Tests concurrent Hot Rod server startups
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodConcurrentStartTest")
class HotRodConcurrentStartTest extends MultipleCacheManagersTest {
   private[this] var hotRodServers: List[HotRodServer] = List()
   private[this] val hotRodClients: List[HotRodClient] = List()
   private val numberOfServers = 2
   private val cacheName = "hotRodConcurrentStart"

   @Test(enabled=false) // Disable explicitly to avoid TestNG thinking this is a test!!
   override def createCacheManagers() {
      for (i <- 0 until numberOfServers) {
         val cm = TestCacheManagerFactory.createClusteredCacheManager(hotRodCacheConfiguration())
         cacheManagers.add(cm)
         val cfg = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false))
         cm.defineConfiguration(cacheName, cfg.build())
      }
   }

   @AfterClass(alwaysRun = true)
   override def destroy() {
      try {
         log.debug("Test finished, close Hot Rod server")
         hotRodClients.foreach(killClient(_))
         hotRodServers.foreach(killServer(_))
      } finally {
         super.destroy() // Stop the caches last so that at stoppage time topology cache can be updated properly
      }
   }

   @AfterMethod(alwaysRun=true)
   override def clearContent() {
      // Do not clear cache between methods so that topology cache does not get cleared
   }

   def testConcurrentStartup() {
      val initialPort = UniquePortThreadLocal.get.intValue
      // Start servers in paralell using Scala's futures
      // Start first server with delay so that cache not found issue can be replicated
      val hotRodServer1 = future(startHotRodServerWithDelay(getCacheManagers.get(0), initialPort, 10000))
      val hotRodServer2 = future(startHotRodServer(getCacheManagers.get(1), initialPort + 10))

      hotRodServers = hotRodServers ::: List(hotRodServer1.apply())
      hotRodServers = hotRodServers ::: List(hotRodServer2.apply())
   }

}