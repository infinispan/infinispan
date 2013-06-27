package org.infinispan.server.hotrod

import test.HotRodClient
import java.lang.reflect.Method
import org.infinispan.test.MultipleCacheManagersTest
import test.HotRodTestingUtil._
import org.infinispan.test.AbstractCacheTest._
import org.testng.annotations.{AfterMethod, Test}
import org.infinispan.server.core.test.ServerTestingUtil._
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.configuration.cache.CacheMode
import org.infinispan.commons.CacheConfigurationException
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder
import org.infinispan.configuration.global.GlobalConfigurationBuilder
import org.infinispan.server.hotrod.test.UniquePortThreadLocal

@Test(groups = Array("functional"), testName = "server.hotrod.HotRodSharedContainerTest")
class HotRodSharedContainerTest extends MultipleCacheManagersTest {

   private var hotRodServer1: HotRodServer = _
   private var hotRodServer2: HotRodServer = _
   private var hotRodClient1: HotRodClient = _
   private var hotRodClient2: HotRodClient = _
   private val cacheName = "HotRodCache"

   @Test(enabled=false) // to avoid TestNG picking it up as a test
   override def createCacheManagers() {
      val globalCfg = GlobalConfigurationBuilder.defaultClusteredBuilder()
      globalCfg.globalJmxStatistics().allowDuplicateDomains(true)
      val cm = TestCacheManagerFactory.createClusteredCacheManager(globalCfg, hotRodCacheConfiguration())
      cacheManagers.add(cm)
      val builder = hotRodCacheConfiguration(
         getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false))
      cm.defineConfiguration(cacheName, builder.build())
   }

   @Test(expectedExceptions= Array(classOf[CacheConfigurationException]))
   def testTopologyConflict() {
      val basePort = UniquePortThreadLocal.get.intValue
      hotRodServer1 = startHotRodServer(cacheManagers.get(0), basePort, new HotRodServerConfigurationBuilder())
      hotRodServer2 = startHotRodServer(cacheManagers.get(0), basePort + 50, new HotRodServerConfigurationBuilder())
   }

   def testSharedContainer(m: Method) {
      val basePort = UniquePortThreadLocal.get.intValue
      hotRodServer1 = startHotRodServer(cacheManagers.get(0), basePort, new HotRodServerConfigurationBuilder().name("1"))
      hotRodServer2 = startHotRodServer(cacheManagers.get(0), basePort + 50, new HotRodServerConfigurationBuilder().name("2"))

      hotRodClient1 = new HotRodClient("127.0.0.1", hotRodServer1.getPort, cacheName, 60, 12)
      hotRodClient2 = new HotRodClient("127.0.0.1", hotRodServer2.getPort, cacheName, 60, 12)

      hotRodClient1.put(k(m) , 0, 0, v(m))
      assertSuccess(hotRodClient2.get(k(m), 0), v(m))
   }

   @AfterMethod(alwaysRun = true)
   def killClientsAndServers() {
      killClient(hotRodClient1)
      killClient(hotRodClient2)
      killServer(hotRodServer1)
      killServer(hotRodServer2)
   }

}