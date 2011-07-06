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
import org.testng.Assert._
import org.testng.annotations.Test
import org.infinispan.test.AbstractCacheTest._

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
      resp = clients.tail.head.ping(2, 1)
      assertStatus(resp, Success)
      assertTopologyReceived(resp.topologyResponse.get, servers)
      resp = clients.tail.head.ping(2, 2)
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
      resp = clients.tail.head.put(k(m) , 0, 0, v(m, "v2-"), 2, 1)
      assertStatus(resp, Success)
      assertTopologyReceived(resp.topologyResponse.get, servers)
      resp = clients.head.put(k(m) , 0, 0, v(m, "v3-"), 2, 2)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v3-"))

      val newServer = startClusteredServer(servers.tail.head.getPort + 25)
      try {
         val resp = clients.head.put(k(m) , 0, 0, v(m, "v4-"), 2, 2)
         assertStatus(resp, Success)
         assertEquals(resp.topologyResponse.get.view.topologyId, 3)
         assertEquals(resp.topologyResponse.get.view.members.size, 3)
         assertAddressEquals(resp.topologyResponse.get.view.members.head, servers.head.getAddress)
         assertAddressEquals(resp.topologyResponse.get.view.members.tail.head, servers.tail.head.getAddress)
         assertAddressEquals(resp.topologyResponse.get.view.members.tail.tail.head, newServer.getAddress)
         assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v4-"))
      } finally {
         stopClusteredServer(newServer)
      }

      resp = clients.head.put(k(m) , 0, 0, v(m, "v5-"), 2, 3)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse.get.view.topologyId, 4)
      assertEquals(resp.topologyResponse.get.view.members.size, 2)
      assertAddressEquals(resp.topologyResponse.get.view.members.head, servers.head.getAddress)
      assertAddressEquals(resp.topologyResponse.get.view.members.tail.head, servers.tail.head.getAddress)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v5-"))

      val crashingServer = startClusteredServer(servers.tail.head.getPort + 25, true)
      try {
         val resp = clients.head.put(k(m) , 0, 0, v(m, "v6-"), 2, 4)
         assertStatus(resp, Success)
         assertEquals(resp.topologyResponse.get.view.topologyId, 5)
         assertEquals(resp.topologyResponse.get.view.members.size, 3)
         assertAddressEquals(resp.topologyResponse.get.view.members.head, servers.head.getAddress)
         assertAddressEquals(resp.topologyResponse.get.view.members.tail.head, servers.tail.head.getAddress)
         assertAddressEquals(resp.topologyResponse.get.view.members.tail.tail.head, crashingServer.getAddress)
         assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v6-"))
      } finally {
         stopClusteredServer(crashingServer)
      }

      resp = clients.head.put(k(m) , 0, 0, v(m, "v7-"), 2, 5)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse.get.view.topologyId, 6)
      assertEquals(resp.topologyResponse.get.view.members.size, 2)
      assertAddressEquals(resp.topologyResponse.get.view.members.head, servers.head.getAddress)
      assertAddressEquals(resp.topologyResponse.get.view.members.tail.head, servers.tail.head.getAddress)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v7-"))

      resp = clients.head.put(k(m) , 0, 0, v(m, "v8-"), 3, 1)
      assertStatus(resp, Success)
      val hashTopologyResp = resp.topologyResponse.get.asInstanceOf[HashDistAwareResponse]
      assertEquals(hashTopologyResp.view.topologyId, 6)
      assertEquals(hashTopologyResp.view.members.size, 2)
      assertAddressEquals(hashTopologyResp.view.members.head, servers.head.getAddress, Map(cacheName -> List(0)))
      assertAddressEquals(hashTopologyResp.view.members.tail.head, servers.tail.head.getAddress, Map(cacheName -> List(0)))
      assertEquals(hashTopologyResp.numOwners, 0)
      assertEquals(hashTopologyResp.hashFunction, 0)
      assertEquals(hashTopologyResp.hashSpace, 0)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v8-"))
   }

}
