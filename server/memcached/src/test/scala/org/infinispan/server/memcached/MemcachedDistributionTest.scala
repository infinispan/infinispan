package org.infinispan.server.memcached

import org.testng.annotations.Test
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.configuration.cache.{CacheMode, ConfigurationBuilder}
import org.infinispan.commons.equivalence.ByteArrayEquivalence
import org.infinispan.test.fwk.TestCacheManagerFactory
import net.spy.memcached.MemcachedClient
import org.infinispan.distribution.DistributionTestHelper
import scala.collection.JavaConversions._
import org.testng.Assert._
import java.util.concurrent.TimeUnit

/**
 * Tests distributed mode with Memcached servers.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Test(groups = Array("functional"), testName = "server.memcached.MemcachedDistributionTest")
class MemcachedDistributionTest extends MemcachedMultiNodeTest {

   protected def createCacheManager(index: Int): EmbeddedCacheManager = {
      val builder = new ConfigurationBuilder
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(1)
              .dataContainer().valueEquivalence(ByteArrayEquivalence.INSTANCE)
      TestCacheManagerFactory.createClusteredCacheManager(builder)
   }

   def testGetFromNonOwner() {
      val owner = getFirstOwner("1")
      val f = owner.set("1", 0, "v1")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      val nonOwner = getFirstNonOwner("1")
      assertEquals(nonOwner.get("1"), "v1")
   }

   private def getFirstNonOwner(k: String): MemcachedClient = getCacheThat(k, owner = false)

   private def getFirstOwner(k: String): MemcachedClient = getCacheThat(k, owner = true)

   private def getCacheThat(k: String, owner: Boolean): MemcachedClient = {
      val caches = servers.map(_.getCacheManager.getCache[String, Array[Byte]](cacheName))
      val cache =
         if (owner) DistributionTestHelper.getFirstOwner[String, Array[Byte]](k, caches)
         else DistributionTestHelper.getFirstNonOwner[String, Array[Byte]](k, caches)

      cacheClient.get(cache).get
   }

}
