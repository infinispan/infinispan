package org.infinispan.server.hotrod

import java.lang.reflect.Method
import org.infinispan.configuration.cache.{CacheMode, ConfigurationBuilder}
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.test.AbstractCacheTest._
import org.infinispan.test.TestingUtil
import org.testng.Assert._
import org.testng.annotations.Test
import test.AbstractTestTopologyAwareResponse
import test.HotRodTestingUtil._

/**
 * Tests Hot Rod instances configured with replication in protocol version 1.0.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodReplicationTest")
class HotRod10ReplicationTest extends HotRodMultiNodeTest {

   override protected def cacheName: String = "hotRodReplSync"

   override protected def createCacheConfig: ConfigurationBuilder = {
      val config = hotRodCacheConfiguration(
         getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false))
      config.clustering().stateTransfer().fetchInMemoryState(true)
      config
   }

   override protected def protocolVersion = 10

   def testReplicatedPut(m: Method) {
      val resp = clients.head.put(k(m) , 0, 0, v(m))
      assertStatus(resp, Success)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m))
   }

   def testReplicatedPutIfAbsent(m: Method) {
      assertKeyDoesNotExist(clients.head.assertGet(m))
      assertKeyDoesNotExist(clients.tail.head.assertGet(m))
      val resp = clients.head.putIfAbsent(k(m) , 0, 0, v(m))
      assertStatus(resp, Success)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m))
      assertStatus(clients.tail.head.putIfAbsent(k(m) , 0, 0, v(m, "v2-")), OperationNotExecuted)
   }

   def testReplicatedReplace(m: Method) {
      var resp = clients.head.replace(k(m), 0, 0, v(m))
      assertStatus(resp, OperationNotExecuted)
      resp = clients.tail.head.replace(k(m), 0, 0, v(m))
      assertStatus(resp , OperationNotExecuted)
      clients.tail.head.assertPut(m)
      resp = clients.tail.head.replace(k(m), 0, 0, v(m, "v1-"))
      assertStatus(resp, Success)
      assertSuccess(clients.head.assertGet(m), v(m, "v1-"))
      resp = clients.head.replace(k(m), 0, 0, v(m, "v2-"))
      assertStatus(resp, Success)
      assertSuccess(clients.tail.head.assertGet(m), v(m, "v2-"))
   }

   def testPingWithTopologyAwareClient() {
      var resp = clients.head.ping
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)

      resp = clients.tail.head.ping(1, 0)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)

      resp = clients.head.ping(2, 0)
      assertStatus(resp, Success)
      assertTopologyReceived(resp.topologyResponse.get, servers, currentServerTopologyId)

      resp = clients.tail.head.ping(2, 0)
      assertStatus(resp, Success)
      assertTopologyReceived(resp.topologyResponse.get, servers, currentServerTopologyId)

      resp = clients.tail.head.ping(2, 3)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)
   }

   def testReplicatedPutWithTopologyChanges(m: Method) {
      var resp = clients.head.put(k(m) , 0, 0, v(m), 1, 0)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m))

      resp = clients.head.put(k(m) , 0, 0, v(m, "v1-"), 2, 0)
      assertStatus(resp, Success)
      assertTopologyReceived(resp.topologyResponse.get, servers, currentServerTopologyId)

      resp = clients.tail.head.put(k(m) , 0, 0, v(m, "v2-"), 2, 0)
      assertStatus(resp, Success)
      assertTopologyReceived(resp.topologyResponse.get, servers, currentServerTopologyId)

      resp = clients.head.put(k(m) , 0, 0, v(m, "v3-"), 2, 3)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v3-"))

      val newServer = startClusteredServer(servers.tail.head.getPort + 25)
      try {
         val resp = clients.head.put(k(m) , 0, 0, v(m, "v4-"), 2, 1)
         assertStatus(resp, Success)
         assertEquals(resp.topologyResponse.get.topologyId, currentServerTopologyId)
         val topoResp = resp.asTopologyAwareResponse
         assertEquals(topoResp.members.size, 3)
         (newServer.getAddress :: servers.map(_.getAddress)).foreach(
            addr => assertTrue(topoResp.members.exists(_ == addr)))
         assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v4-"))
      } finally {
         stopClusteredServer(newServer)
         TestingUtil.waitForRehashToComplete(cache(0, cacheName), cache(1, cacheName))
      }

      resp = clients.head.put(k(m) , 0, 0, v(m, "v5-"), 2, 2)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse.get.topologyId, currentServerTopologyId)
      var topoResp = resp.asTopologyAwareResponse
      assertEquals(topoResp.members.size, 2)
      servers.map(_.getAddress).foreach(
         addr => assertTrue(topoResp.members.exists(_ == addr)))
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v5-"))

      val crashingServer = startClusteredServer(
            servers.tail.head.getPort + 25, doCrash = true)
      try {
         val resp = clients.head.put(k(m) , 0, 0, v(m, "v6-"), 2, 3)
         assertStatus(resp, Success)
         assertEquals(resp.topologyResponse.get.topologyId, currentServerTopologyId)
         val topoResp = resp.asTopologyAwareResponse
         assertEquals(topoResp.members.size, 3)
         (crashingServer.getAddress :: servers.map(_.getAddress)).foreach(
            addr => assertTrue(topoResp.members.exists(_ == addr)))
         assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v6-"))
      } finally {
         stopClusteredServer(crashingServer)
         TestingUtil.waitForRehashToComplete(cache(0, cacheName), cache(1, cacheName))
      }

      resp = clients.head.put(k(m) , 0, 0, v(m, "v7-"), 2, 4)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse.get.topologyId, currentServerTopologyId)
      topoResp = resp.asTopologyAwareResponse
      assertEquals(topoResp.members.size, 2)
      servers.map(_.getAddress).foreach(
         addr => assertTrue(topoResp.members.exists(_ == addr)))
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v7-"))

      resp = clients.head.put(k(m) , 0, 0, v(m, "v8-"), 3, 1)
      assertStatus(resp, Success)

      checkTopologyReceived(resp.topologyResponse.get, servers, cacheName)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v8-"))
   }

   @Test(enabled=false) // Disable explicitly to avoid TestNG thinking this is a test!!
   protected def checkTopologyReceived(topoResp: AbstractTestTopologyAwareResponse,
           servers: List[HotRodServer], cacheName: String) {
      assertNoHashTopologyReceived(topoResp, servers, cacheName, currentServerTopologyId)
   }

}
