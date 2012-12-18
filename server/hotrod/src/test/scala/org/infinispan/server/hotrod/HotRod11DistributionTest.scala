/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.server.hotrod

import org.infinispan.config.Configuration
import org.infinispan.test.AbstractCacheTest._
import org.infinispan.config.Configuration.CacheMode
import java.lang.reflect.Method
import test.HotRodTestingUtil._
import org.testng.Assert._
import test.HotRodClient
import org.infinispan.server.hotrod.OperationStatus._
import org.testng.annotations.Test

/**
 * Tests Hot Rod distribution mode using Hot Rod's 1.1 protocol.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRod11DistributionTest")
class HotRod11DistributionTest extends HotRodMultiNodeTest {

   override protected def cacheName = "distributedVersion11"

   override protected def createCacheConfig: Configuration = {
      val cfg = getDefaultClusteredConfig(CacheMode.DIST_SYNC)
      cfg.fluent().l1().disable() // Disable L1 explicitly
      cfg
   }

   override protected def protocolVersion : Byte = 11

   protected def virtualNodes = 1

   def testDistributedPutWithTopologyChanges(m: Method) {
      val client1 = clients.head
      val client2 = clients.tail.head

      var resp = client1.ping(3, 0)
      assertStatus(resp, Success)
      assertHashTopologyReceived(resp.topologyResponse.get, servers, cacheName, 2, virtualNodes)

      resp = client1.put(k(m) , 0, 0, v(m), 1, 0)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)
      assertSuccess(client2.get(k(m), 0), v(m))

      resp = client1.put(k(m) , 0, 0, v(m, "v1-"), 2, 0)
      assertStatus(resp, Success)
      assertTopologyReceived(resp.topologyResponse.get, servers)

      resp = client2.put(k(m) , 0, 0, v(m, "v2-"), 2, 0)
      assertStatus(resp, Success)
      assertTopologyReceived(resp.topologyResponse.get, servers)

      resp = client1.put(k(m) , 0, 0, v(m, "v3-"), 2, 1)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)
      assertSuccess(client2.get(k(m), 0), v(m, "v3-"))

      resp = client1.put(k(m) , 0, 0, v(m, "v4-"), 3, 0)
      assertStatus(resp, Success)
      assertHashTopologyReceived(resp.topologyResponse.get, servers, cacheName, 2, virtualNodes)
      assertSuccess(client2.get(k(m), 0), v(m, "v4-"))

      resp = client2.put(k(m) , 0, 0, v(m, "v5-"), 3, 0)
      assertStatus(resp, Success)
      assertHashTopologyReceived(resp.topologyResponse.get, servers, cacheName, 2, virtualNodes)
      assertSuccess(client2.get(k(m), 0), v(m, "v5-"))

      val newServer = startClusteredServer(servers.tail.head.getPort + 25)
      val newClient = new HotRodClient(
            "127.0.0.1", newServer.getPort, cacheName, 60, protocolVersion)
      val allServers = newServer :: servers
      val addressRemovalLatches = getAddressCacheRemovalLatches(servers)
      try {
         log.trace("New client started, modify key to be v6-*")
         resp = newClient.put(k(m) , 0, 0, v(m, "v6-"), 3, 0)
         assertStatus(resp, Success)
         assertHashTopologyReceived(resp.topologyResponse.get, allServers, cacheName, 2, virtualNodes)

         log.trace("Get key from other client and verify that's v6-*")
         assertSuccess(client2.get(k(m), 0), v(m, "v6-"))

         resp = client2.put(k(m), 0, 0, v(m, "v7-"), 3, 0)
         assertStatus(resp, Success)
         assertHashTopologyReceived(resp.topologyResponse.get, allServers, cacheName, 2, virtualNodes)

         assertSuccess(newClient.get(k(m), 0), v(m, "v7-"))
      } finally {
         log.trace("Stopping new server")
         killClient(newClient)
         stopClusteredServer(newServer)
         waitAddressCacheRemoval(addressRemovalLatches)
         log.trace("New server stopped")
      }

      resp = client2.put(k(m) , 0, 0, v(m, "v8-"), 3, 2)
      assertStatus(resp, Success)
      assertHashTopologyReceived(resp.topologyResponse.get, servers, cacheName, 2, virtualNodes)

      assertSuccess(client1.get(k(m), 0), v(m, "v8-"))
   }


}