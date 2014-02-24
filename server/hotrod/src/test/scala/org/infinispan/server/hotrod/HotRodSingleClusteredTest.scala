package org.infinispan.server.hotrod

import test.HotRodClient
import java.lang.reflect.Method
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.test.MultipleCacheManagersTest
import test.HotRodTestingUtil._
import org.infinispan.test.AbstractCacheTest._
import org.testng.annotations.{AfterClass, BeforeClass, Test}
import org.infinispan.server.core.test.ServerTestingUtil._
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.configuration.cache.CacheMode
import org.infinispan.commons.equivalence.ByteArrayEquivalence

@Test(groups = Array("functional"), testName = "server.hotrod.HotRodSingleClusteredTest")
class HotRodSingleClusteredTest extends MultipleCacheManagersTest {

   private var hotRodServer: HotRodServer = _
   private var hotRodClient: HotRodClient = _
   private val cacheName = "HotRodCache"

   @Test(enabled=false) // Disable explicitly to avoid TestNG thinking this is a test!!
   override def createCacheManagers() {
      val cm = TestCacheManagerFactory.createClusteredCacheManager(hotRodCacheConfiguration())
      cacheManagers.add(cm)
      val builder = hotRodCacheConfiguration(
         getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false))
      cm.defineConfiguration(cacheName, builder.build())
   }

   @BeforeClass(alwaysRun = true)
   @Test(enabled=false) // Disable explicitly to avoid TestNG thinking this is a test!!
   override def createBeforeClass() {
      super.createBeforeClass()
      hotRodServer = startHotRodServer(cacheManagers.get(0))
      hotRodClient = new HotRodClient("127.0.0.1", hotRodServer.getPort, cacheName, 60, 20)
   }

   @AfterClass(alwaysRun = true)
   override def destroy() {
      log.debug("Test finished, close client, server, and cache managers")
      killClient(hotRodClient)
      killServer(hotRodServer)
      super.destroy()
   }

   def testPutGet(m: Method) {
      assertStatus(hotRodClient.put(k(m) , 0, 0, v(m)), Success)
      assertSuccess(hotRodClient.get(k(m), 0), v(m))
   }


}