package org.infinispan.server.hotrod

import org.infinispan.configuration.cache.{CacheMode, ConfigurationBuilder}
import org.infinispan.test.AbstractCacheTest._
import org.testng.annotations.Test
import java.lang.reflect.Method
import test.HotRodTestingUtil._
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.util.ByteArrayEquivalence

/**
 * Tests behaviour of Hot Rod servers with asymmetric clusters
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodAsymmetricClusterTest")
class HotRodAsymmetricClusterTest extends HotRodMultiNodeTest {

  protected def cacheName: String = "asymmetricCache"

  protected def createCacheConfig: ConfigurationBuilder =
     hotRodCacheConfiguration(
        getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false))


  @Test(enabled = false)
  override def createCacheManagers() {
     for (i <- 0 until 2) {
        val cm = TestCacheManagerFactory.createClusteredCacheManager(hotRodCacheConfiguration())
        cacheManagers.add(cm)
        if (i == 0) {
           cm.defineConfiguration(cacheName, createCacheConfig.build())
        }
     }
  }

   protected def protocolVersion: Byte = 12

   def testPutInCacheDefinedNode(m: Method) {
      val resp = clients.head.put(k(m) , 0, 0, v(m))
      assertStatus(resp, Success)
   }

   def testPutInNonCacheDefinedNode(m: Method) {
      val resp = clients.tail.head.put(k(m) , 0, 0, v(m))
      assertStatus(resp, ParseError)
   }

}
