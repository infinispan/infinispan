package org.infinispan.server.hotrod

import test.{HotRodClient, TestErrorResponse}
import java.lang.reflect.Method
import java.util.EnumSet

import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.test.MultipleCacheManagersTest
import test.HotRodTestingUtil._
import org.infinispan.test.AbstractCacheTest._
import org.testng.annotations.{AfterClass, BeforeClass, Test}
import org.infinispan.server.core.test.ServerTestingUtil._
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.configuration.cache.{CacheMode, ConfigurationBuilder}
import org.infinispan.commons.equivalence.ByteArrayEquivalence
import org.infinispan.registry.InternalCacheRegistry
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration
import org.testng.Assert._

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

   def testPutOnPrivateCache(m: Method) {
      val resp = hotRodClient.execute(0xA0, 0x01, hotRodServer.getConfiguration.topologyCacheName(), k(m), 0, 0, v(m), 0, 1, 0).asInstanceOf[TestErrorResponse]
      assertTrue(resp.msg.contains("Remote requests are not allowed to private caches."))
      assertEquals(resp.status, ParseError, "Status should have been 'ParseError' but instead was: " + resp.status)
      hotRodClient.assertPut(m)
   }

   def testLoopbackPutOnProtectedCache(m: Method) {
      val internalCacheRegistry = manager(0).getGlobalComponentRegistry.getComponent(classOf[InternalCacheRegistry])
      internalCacheRegistry.registerInternalCache("MyInternalCache",
         new ConfigurationBuilder().build(),
         EnumSet.of(InternalCacheRegistry.Flag.USER, InternalCacheRegistry.Flag.PROTECTED))
      val resp = hotRodClient.execute(0xA0, 0x01, "MyInternalCache", k(m), 0, 0, v(m), 0, 1, 0)
      assertEquals(Success, resp.status)
   }


}
