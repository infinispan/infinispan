package org.infinispan.server.hotrod

import org.testng.annotations.Test
import org.infinispan.config.Configuration.CacheMode
import org.infinispan.config.Configuration
import java.lang.reflect.Method
import org.infinispan.server.hotrod.OperationStatus._
import test.HotRodClient
import test.HotRodTestingUtil._
import org.testng.Assert._
import collection.mutable.ListBuffer
import org.infinispan.distribution.UnionConsistentHash
import org.infinispan.test.TestingUtil
import org.infinispan.test.AbstractCacheTest._

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodDistributionTest")
class HotRodDistributionTest extends HotRodMultiNodeTest {

   override protected def cacheName: String = "hotRodDistSync"

   override protected def createCacheConfig: Configuration = getDefaultClusteredConfig(CacheMode.DIST_SYNC)

   def testDistributedPutWithTopologyChanges(m: Method) {
      var resp = clients.head.put(k(m) , 0, 0, v(m), 1, 0)
      assertStatus(resp.status, Success)
      assertEquals(resp.topologyResponse, None)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m))
      resp = clients.head.put(k(m) , 0, 0, v(m, "v1-"), 2, 0)
      assertStatus(resp.status, Success)
      assertTopologyReceived(resp.topologyResponse.get, servers)
      resp = clients.tail.head.put(k(m) , 0, 0, v(m, "v2-"), 2, 1)
      assertStatus(resp.status, Success)
      assertTopologyReceived(resp.topologyResponse.get, servers)
      resp = clients.head.put(k(m) , 0, 0, v(m, "v3-"), 2, 2)
      assertStatus(resp.status, Success)
      assertEquals(resp.topologyResponse, None)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v3-"))

      resp = clients.head.put(k(m) , 0, 0, v(m, "v4-"), 3, 0)
      assertStatus(resp.status, Success)
      var expectedHashIds = generateExpectedHashIds
      assertHashTopologyReceived(resp.topologyResponse.get, servers, expectedHashIds)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v4-"))
      resp = clients.tail.head.put(k(m) , 0, 0, v(m, "v5-"), 3, 1)
      assertStatus(resp.status, Success)
      expectedHashIds = generateExpectedHashIds
      assertHashTopologyReceived(resp.topologyResponse.get, servers, expectedHashIds)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v5-"))

      var cm = addClusterEnabledCacheManager()
      cm.defineConfiguration(cacheName, createCacheConfig)
      val newServer = startHotRodServer(cm, servers.tail.head.getPort + 25)
      val newClient = new HotRodClient("127.0.0.1", newServer.getPort, cacheName, 60)
      try {
         log.trace("New client started, modify key to be v6-*", null)
         resp = newClient.put(k(m) , 0, 0, v(m, "v6-"), 3, 2)
         // resp = clients.tail.head.put(k(m) , 0, 0, v(m, "v6-"), 3, 2)
         assertStatus(resp.status, Success)
         val hashTopologyResp = resp.topologyResponse.get.asInstanceOf[HashDistAwareResponse]
         assertEquals(hashTopologyResp.view.topologyId, 3)
         assertEquals(hashTopologyResp.view.members.size, 3)
         val consistentHash = cacheManagers.get(2).getCache(cacheName).getAdvancedCache.getDistributionManager.getConsistentHash
         assertAddressEquals(hashTopologyResp.view.members.head, servers.head.getAddress,
            Map(cacheName -> consistentHash.getHashId(servers.head.getAddress.clusterAddress)))
         assertAddressEquals(hashTopologyResp.view.members.tail.head, servers.tail.head.getAddress,
            Map(cacheName -> consistentHash.getHashId(servers.tail.head.getAddress.clusterAddress)))
         assertAddressEquals(hashTopologyResp.view.members.tail.tail.head, newServer.getAddress,
            Map(cacheName -> consistentHash.getHashId(newServer.getAddress.clusterAddress)))
         assertEquals(hashTopologyResp.numOwners, 2)
         assertEquals(hashTopologyResp.hashFunction, 1)
         assertEquals(hashTopologyResp.hashSpace, 10240)
         log.trace("Get key and verify that's v6-*", null)
         assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v6-"))
      } finally {
         newClient.stop
         newServer.stop
         cm.stop
      }

      resp = clients.tail.head.put(k(m) , 0, 0, v(m, "v7-"), 3, 3)
      assertStatus(resp.status, Success)
      val hashTopologyResp = resp.topologyResponse.get.asInstanceOf[HashDistAwareResponse]
      assertEquals(hashTopologyResp.view.topologyId, 4)
      assertEquals(hashTopologyResp.view.members.size, 2)
      val consistentHash = cacheManagers.get(1).getCache(cacheName).getAdvancedCache.getDistributionManager.getConsistentHash
      assertAddressEquals(hashTopologyResp.view.members.head, servers.head.getAddress,
         Map(cacheName -> consistentHash.getHashId(servers.head.getAddress.clusterAddress)))
      assertAddressEquals(hashTopologyResp.view.members.tail.head, servers.tail.head.getAddress,
         Map(cacheName -> consistentHash.getHashId(servers.tail.head.getAddress.clusterAddress)))
      assertEquals(hashTopologyResp.numOwners, 2)
      assertEquals(hashTopologyResp.hashFunction, 1)
      assertEquals(hashTopologyResp.hashSpace, 10240)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v7-"))
   }

   private def generateExpectedHashIds: List[Map[String, Int]] = {
      val listBuffer = new ListBuffer[Map[String, Int]]
      val consistentHash = cacheManagers.get(0).getCache(cacheName).getAdvancedCache.getDistributionManager.getConsistentHash
      var i = 0
      while (consistentHash.isInstanceOf[UnionConsistentHash] && i < 10) {
         TestingUtil.sleepThread(1000)
         i += 1
      }
      listBuffer += Map(cacheName -> consistentHash.getHashId(servers.head.getAddress.clusterAddress))
      listBuffer += Map(cacheName -> consistentHash.getHashId(servers.tail.head.getAddress.clusterAddress))
      listBuffer.toList
   }
}