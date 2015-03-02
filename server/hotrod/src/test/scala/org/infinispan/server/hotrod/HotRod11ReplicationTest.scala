package org.infinispan.server.hotrod

import org.infinispan.test.AbstractCacheTest._
import java.lang.reflect.Method
import test.HotRodTestingUtil._
import org.testng.Assert._
import test.HotRodClient
import org.infinispan.server.hotrod.OperationStatus._
import org.testng.annotations.Test
import org.infinispan.configuration.cache.{CacheMode, ConfigurationBuilder}
import org.infinispan.server.hotrod.Constants._
import org.infinispan.test.TestingUtil
import org.infinispan.commons.equivalence.ByteArrayEquivalence

/**
 * Tests Hot Rod replication mode using Hot Rod's 1.1 protocol.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRod11ReplicationTest")
class HotRod11ReplicationTest extends HotRodMultiNodeTest {

   override protected def cacheName = "replicateVersion11"

   override protected def createCacheConfig: ConfigurationBuilder =
      hotRodCacheConfiguration(
         getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false))

   override protected def protocolVersion : Byte = 11

   protected def virtualNodes = 1

   def testDistributedPutWithTopologyChanges(m: Method) {
      val client1 = clients.head
      val client2 = clients.tail.head

      var resp = client1.ping(INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0)
      assertStatus(resp, Success)
      assertTopologyReceived(resp.topologyResponse.get, servers, currentServerTopologyId)

      // Client intelligence is now 1, which means no topology updates
      resp = client1.put(k(m) , 0, 0, v(m), INTELLIGENCE_BASIC, 0)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)
      assertSuccess(client2.get(k(m), 0), v(m))

      resp = client1.put(k(m) , 0, 0, v(m, "v1-"), INTELLIGENCE_TOPOLOGY_AWARE, 0)
      assertStatus(resp, Success)
      assertTopologyReceived(resp.topologyResponse.get, servers, currentServerTopologyId)

      resp = client2.put(k(m) , 0, 0, v(m, "v2-"), INTELLIGENCE_TOPOLOGY_AWARE, 0)
      assertStatus(resp, Success)
      assertTopologyReceived(resp.topologyResponse.get, servers, currentServerTopologyId)

      resp = client1.put(k(m) , 0, 0, v(m, "v3-"), INTELLIGENCE_TOPOLOGY_AWARE, 3)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)
      assertSuccess(client2.get(k(m), 0), v(m, "v3-"))

      resp = client1.put(k(m) , 0, 0, v(m, "v4-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0)
      assertStatus(resp, Success)
      assertTopologyReceived(resp.topologyResponse.get, servers, currentServerTopologyId)
      assertSuccess(client2.get(k(m), 0), v(m, "v4-"))

      resp = client2.put(k(m) , 0, 0, v(m, "v5-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0)
      assertStatus(resp, Success)
      assertTopologyReceived(resp.topologyResponse.get, servers, currentServerTopologyId)
      assertSuccess(client2.get(k(m), 0), v(m, "v5-"))

      val newServer = startClusteredServer(servers.tail.head.getPort + 25)
      val newClient = new HotRodClient(
            "127.0.0.1", newServer.getPort, cacheName, 60, protocolVersion)
      val allServers = newServer :: servers
      try {
         log.trace("New client started, modify key to be v6-*")
         resp = newClient.put(k(m) , 0, 0, v(m, "v6-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0)
         assertStatus(resp, Success)
         assertTopologyReceived(resp.topologyResponse.get, allServers, currentServerTopologyId)

         log.trace("Get key from other client and verify that's v6-*")
         assertSuccess(client2.get(k(m), 0), v(m, "v6-"))

         resp = client2.put(k(m), 0, 0, v(m, "v7-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0)
         assertStatus(resp, Success)
         assertTopologyReceived(resp.topologyResponse.get, allServers, currentServerTopologyId)

         assertSuccess(newClient.get(k(m), 0), v(m, "v7-"))
      } finally {
         log.trace("Stopping new server")
         killClient(newClient)
         stopClusteredServer(newServer)
         TestingUtil.waitForRehashToComplete(cache(0, cacheName), cache(1, cacheName))
         log.trace("New server stopped")
      }

      resp = client2.put(k(m) , 0, 0, v(m, "v8-"), 3, 2)
      assertStatus(resp, Success)
      assertTopologyReceived(resp.topologyResponse.get, servers, currentServerTopologyId)

      assertSuccess(client1.get(k(m), 0), v(m, "v8-"))
   }


}
