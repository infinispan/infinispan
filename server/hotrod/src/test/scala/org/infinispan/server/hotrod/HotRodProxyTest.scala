package org.infinispan.server.hotrod

import org.infinispan.manager.EmbeddedCacheManager
import test.HotRodTestingUtil._
import org.infinispan.server.hotrod.OperationStatus._
import org.testng.annotations.Test
import org.testng.Assert._
import org.infinispan.test.AbstractCacheTest._
import org.infinispan.configuration.cache.{CacheMode, ConfigurationBuilder}
import org.infinispan.commons.equivalence.ByteArrayEquivalence
import org.infinispan.server.hotrod.test.HotRodTestingUtil

/**
 * Tests Hot Rod instances that are behind a proxy.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodProxyTest")
class HotRodProxyTest extends HotRodMultiNodeTest {

   private val proxyHost1 = "1.2.3.4"
   private val proxyHost2 = "2.3.4.5"
   private val proxyPort1 = 8123
   private val proxyPort2 = 9123

   override protected def cacheName: String = "hotRodProxy"

   override protected def createCacheConfig: ConfigurationBuilder = {
      val config = hotRodCacheConfiguration(
         getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false))
      config.clustering().stateTransfer().fetchInMemoryState(true)
      config
   }

   override protected def startTestHotRodServer(cacheManager: EmbeddedCacheManager, port: Int) = {
      if (port == HotRodTestingUtil.serverPort)
         startHotRodServer(cacheManager, proxyHost1, proxyPort1)
      else
         startHotRodServer(cacheManager, port, proxyHost2, proxyPort2)
   }

   def testTopologyWithProxiesReturned() {
      val resp = clients.head.ping(2, 0)
      assertStatus(resp, Success)
      val topoResp = resp.asTopologyAwareResponse
      assertEquals(topoResp.topologyId, currentServerTopologyId)
      assertEquals(topoResp.members.size, 2)
      topoResp.members.foreach(member => servers.map(_.getAddress).exists(_ == member))
   }

}
