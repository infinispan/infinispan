/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.server.hotrod

import org.infinispan.config.Configuration
import java.lang.reflect.Method
import test.HotRodTestingUtil._
import org.infinispan.server.hotrod.OperationStatus._
import org.testng.annotations.Test
import org.infinispan.test.AbstractCacheTest._
import org.testng.Assert._
import test.TestHashDistAware10Response

/**
 * Tests Hot Rod instances configured with replication.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodReplicationTest")
class HotRodReplicationTest extends HotRodMultiNodeTest {

   override protected def cacheName: String = "hotRodReplSync"

   override protected def createCacheConfig: Configuration = {
      val config = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC)
      config.setFetchInMemoryState(true)
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

   def testPingWithTopologyAwareClient {
      var resp = clients.head.ping
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)

      resp = clients.tail.head.ping(1, 0)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)

      resp = clients.head.ping(2, 0)
      assertStatus(resp, Success)
      assertTopologyReceived(resp.topologyResponse.get, servers)

      resp = clients.tail.head.ping(2, 0)
      assertStatus(resp, Success)
      assertTopologyReceived(resp.topologyResponse.get, servers)

      resp = clients.tail.head.ping(2, 1)
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
      assertTopologyReceived(resp.topologyResponse.get, servers)

      resp = clients.tail.head.put(k(m) , 0, 0, v(m, "v2-"), 2, 0)
      assertStatus(resp, Success)
      assertTopologyReceived(resp.topologyResponse.get, servers)

      resp = clients.head.put(k(m) , 0, 0, v(m, "v3-"), 2, 1)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v3-"))

      val newServer = startClusteredServer(servers.tail.head.getPort + 25)
      var addressRemovalLatches = getAddressCacheRemovalLatches(servers)
      try {
         val resp = clients.head.put(k(m) , 0, 0, v(m, "v4-"), 2, 1)
         assertStatus(resp, Success)
         assertTopologyId(resp.topologyResponse.get.viewId, cacheManagers.get(0))
         val topoResp = resp.asTopologyAwareResponse
         assertEquals(topoResp.members.size, 3)
         (newServer.getAddress :: servers.map(_.getAddress)).foreach(
            addr => assertTrue(topoResp.members.exists(_ == addr)))
         assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v4-"))
      } finally {
         stopClusteredServer(newServer)
         waitAddressCacheRemoval(addressRemovalLatches)
      }

      resp = clients.head.put(k(m) , 0, 0, v(m, "v5-"), 2, 2)
      assertStatus(resp, Success)
      assertTopologyId(resp.topologyResponse.get.viewId, cacheManagers.get(0))
      var topoResp = resp.asTopologyAwareResponse
      assertEquals(topoResp.members.size, 2)
      servers.map(_.getAddress).foreach(
         addr => assertTrue(topoResp.members.exists(_ == addr)))
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v5-"))

      val crashingServer = startClusteredServer(servers.tail.head.getPort + 25, true)
      addressRemovalLatches = getAddressCacheRemovalLatches(servers)
      try {
         val resp = clients.head.put(k(m) , 0, 0, v(m, "v6-"), 2, 3)
         assertStatus(resp, Success)
         assertTopologyId(resp.topologyResponse.get.viewId, cacheManagers.get(0))
         val topoResp = resp.asTopologyAwareResponse
         assertEquals(topoResp.members.size, 3)
         (crashingServer.getAddress :: servers.map(_.getAddress)).foreach(
            addr => assertTrue(topoResp.members.exists(_ == addr)))
         assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v6-"))
      } finally {
         stopClusteredServer(crashingServer)
         waitAddressCacheRemoval(addressRemovalLatches)
      }

      resp = clients.head.put(k(m) , 0, 0, v(m, "v7-"), 2, 4)
      assertStatus(resp, Success)
      assertTopologyId(resp.topologyResponse.get.viewId, cacheManagers.get(0))
      topoResp = resp.asTopologyAwareResponse
      assertEquals(topoResp.members.size, 2)
      servers.map(_.getAddress).foreach(
         addr => assertTrue(topoResp.members.exists(_ == addr)))
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v7-"))

      resp = clients.head.put(k(m) , 0, 0, v(m, "v8-"), 3, 1)
      assertStatus(resp, Success)
      val hashTopologyResp = resp.topologyResponse.get.asInstanceOf[TestHashDistAware10Response]
      assertTopologyId(resp.topologyResponse.get.viewId, cacheManagers.get(0))
      assertEquals(hashTopologyResp.members.size, 2)
      servers.map(_.getAddress).foreach(
         addr => assertTrue(hashTopologyResp.members.exists(_ == addr)))
      val expectedHashIds = Map(servers.head.getAddress -> List(0), servers.tail.head.getAddress -> List(0))
      assertHashIds(hashTopologyResp.hashIds, expectedHashIds)

      assertEquals(hashTopologyResp.numOwners, 0)
      assertEquals(hashTopologyResp.hashFunction, 0)
      assertEquals(hashTopologyResp.hashSpace, 0)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v8-"))
   }

}
