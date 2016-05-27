package org.infinispan.server.hotrod

import java.lang.reflect.Method
import java.net.NetworkInterface
import java.util.EnumSet

import org.infinispan.configuration.cache.{CacheMode, ConfigurationBuilder}
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.registry.InternalCacheRegistry
import org.infinispan.server.core.test.ServerTestingUtil._
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod.configuration.{HotRodServerConfiguration, HotRodServerConfigurationBuilder}
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.infinispan.server.hotrod.test.{HotRodClient, TestErrorResponse}
import org.infinispan.test.AbstractCacheTest._
import org.infinispan.test.MultipleCacheManagersTest
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.testng.Assert._
import org.testng.annotations.{AfterClass, BeforeClass, Test}

@Test(groups = Array("functional"), testName = "server.hotrod.HotRodSingleClusteredTest")
class HotRodSingleClusteredNonLoopbackTest extends MultipleCacheManagersTest {

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
      val iface = findNetworkInterfaces(false).next
      val address = iface.getInetAddresses.nextElement.getHostAddress
      hotRodServer = startHotRodServer(cacheManagers.get(0), address, serverPort, 0, getDefaultHotRodConfiguration())
      hotRodClient = new HotRodClient(address, hotRodServer.getPort, cacheName, 60, 20)
   }

   @AfterClass(alwaysRun = true)
   override def destroy() {
      log.debug("Test finished, close client, server, and cache managers")
      killClient(hotRodClient)
      killServer(hotRodServer)
      super.destroy()
   }

   def testNonLoopbackPutOnProtectedCache(m: Method) {
      val internalCacheRegistry = manager(0).getGlobalComponentRegistry.getComponent(classOf[InternalCacheRegistry])
      internalCacheRegistry.registerInternalCache("MyInternalCache",
         new ConfigurationBuilder().build(),
         EnumSet.of(InternalCacheRegistry.Flag.USER, InternalCacheRegistry.Flag.PROTECTED))
      val resp = hotRodClient.execute(0xA0, 0x01, "MyInternalCache", k(m), 0, 0, v(m), 0, 1, 0).asInstanceOf[TestErrorResponse]
      assertTrue(resp.msg.contains("protected caches only over loopback"))
      assertEquals(resp.status, ParseError, "Status should have been 'ParseError' but instead was: " + resp.status)
      hotRodClient.assertPut(m)
   }


}