package org.infinispan.server.hotrod

import java.lang.reflect.Method

import org.infinispan.configuration.cache.{CacheMode, ConfigurationBuilder}
import org.infinispan.server.hotrod.Constants._
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod.test.{HotRodMagicKeyGenerator, HotRodClient}
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.infinispan.stats.impl.ClusterCacheStatsImpl
import org.infinispan.test.AbstractCacheTest._
import org.infinispan.test.TestingUtil
import org.testng.Assert._
import org.testng.annotations.Test

@Test(groups = Array("functional"), testName = "server.hotrod.HotRodStatsClusterTest")
class HotRodStatsClusterTest extends HotRodMultiNodeTest {

   override protected def protocolVersion: Byte = 24

   override protected def cacheName: String = "hotRodClusterStats"

   override protected def createCacheConfig: ConfigurationBuilder = {
      val config = hotRodCacheConfiguration(
         getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false))
      config.jmxStatistics().enable()
      config.clustering().hash().numOwners(1)
      config
   }

   def testClusterStats(m: Method): Unit = {
      val client1 = clients.head
      val client2 = clients.tail.head

      val key1 = HotRodMagicKeyGenerator.newKey(cache(0, cacheName))
      val value = v(m, "v1-")
      val resp = client1.put(key1, 0, 0, value, INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0)
      assertStatus(resp, Success)
      assertSuccess(client1.get(key1, 0), value)
      client1.remove(k(m))

      var stats1 = client1.stats
      assertEquals(stats1.get("currentNumberOfEntries"), "1")
      assertEquals(stats1.get("totalNumberOfEntries"), "1")
      assertEquals(stats1.get("stores"), "1")
      assertEquals(stats1.get("hits"), "1")
      assertEquals(stats1.get("retrievals"), "1")
      assertEquals(stats1.get("removeMisses"), "1")
      assertEquals(stats1.get("globalCurrentNumberOfEntries"), "1")
      assertEquals(stats1.get("globalStores"), "1")
      assertEquals(stats1.get("globalHits"), "1")
      assertEquals(stats1.get("globalRetrievals"), "1")
      assertEquals(stats1.get("globalRemoveMisses"), "1")

      var stats2 = client2.stats
      assertEquals(stats2.get("currentNumberOfEntries"), "0")
      assertEquals(stats2.get("totalNumberOfEntries"), "0")
      assertEquals(stats2.get("stores"), "0")
      assertEquals(stats2.get("hits"), "0")
      assertEquals(stats2.get("retrievals"), "0")
      assertEquals(stats2.get("removeMisses"), "0")
      assertEquals(stats2.get("globalCurrentNumberOfEntries"), "1")
      assertEquals(stats2.get("globalStores"), "1")
      assertEquals(stats2.get("globalHits"), "1")
      assertEquals(stats2.get("globalRetrievals"), "1")
      assertEquals(stats2.get("globalRemoveMisses"), "1")

      TestingUtil.sleepThread(ClusterCacheStatsImpl.DEFAULT_STALE_STATS_THRESHOLD + 2000)

      client1.remove(key1)
      assertKeyDoesNotExist(client1.get(key1, 0))

      stats1 = client1.stats
      assertEquals(stats1.get("misses"), "1")
      assertEquals(stats1.get("removeHits"), "1")
      assertEquals(stats1.get("globalMisses"), "1")
      assertEquals(stats1.get("globalRemoveHits"), "1")

      stats2 = client2.stats
      assertEquals(stats2.get("misses"), "0")
      assertEquals(stats2.get("removeHits"), "0")
      assertEquals(stats2.get("globalMisses"), "1")
      assertEquals(stats2.get("globalRemoveHits"), "1")
   }

}
