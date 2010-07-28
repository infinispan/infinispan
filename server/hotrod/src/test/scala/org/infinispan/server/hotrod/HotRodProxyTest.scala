package org.infinispan.server.hotrod

import org.infinispan.manager.EmbeddedCacheManager
import test.HotRodTestingUtil._
import org.infinispan.server.hotrod.OperationStatus._
import org.testng.annotations.Test
import org.testng.Assert._
import org.infinispan.config.Configuration
import org.infinispan.test.AbstractCacheTest._

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

   override protected def createCacheConfig: Configuration = {
      val config = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC)
      config.setFetchInMemoryState(true)
      config
   }

   override protected def startTestHotRodServer(cacheManager: EmbeddedCacheManager) =
      startHotRodServer(cacheManager, proxyHost1, proxyPort1)

   override protected def startTestHotRodServer(cacheManager: EmbeddedCacheManager, port: Int) =
      startHotRodServer(cacheManager, port, proxyHost2, proxyPort2)

   def testTopologyWithProxiesReturned {
      val resp = clients.head.ping(2, 0)
      assertStatus(resp.status, Success)
      val topoResp = resp.topologyResponse.get
      assertEquals(topoResp.view.topologyId, 2)
      assertEquals(topoResp.view.members.size, 2)
      assertEquals(topoResp.view.members.head.host, proxyHost1)
      assertEquals(topoResp.view.members.head.port, proxyPort1)
      assertEquals(topoResp.view.members.tail.head.host, proxyHost2)
      assertEquals(topoResp.view.members.tail.head.port, proxyPort2)
   }

}
