package org.infinispan.server.hotrod

import org.testng.annotations.Test
import org.infinispan.config.Configuration
import test.HotRodClient
import java.lang.reflect.Method
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.test.MultipleCacheManagersTest
import test.HotRodTestingUtil._
import org.infinispan.test.AbstractCacheTest._

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodSingleClusteredTest")
class HotRodSingleClusteredTest extends MultipleCacheManagersTest {

   private var hotRodServer: HotRodServer = _
   private var hotRodClient: HotRodClient = _
   private val cacheName = "HotRodCache"

   @Test(enabled=false) // Disable explicitly to avoid TestNG thinking this is a test!!
   override def createCacheManagers {
      val cm = addClusterEnabledCacheManager()
      cm.defineConfiguration(cacheName, getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC))
      hotRodServer = startHotRodServer(cm)
      hotRodClient = new HotRodClient("127.0.0.1", hotRodServer.getPort, cacheName, 60)
   }

   def testPutGet(m: Method) {
      val putSt = hotRodClient.put(k(m) , 0, 0, v(m)).status
      assertStatus(putSt, Success)
      assertSuccess(hotRodClient.get(k(m), 0), v(m))
   }

   
}