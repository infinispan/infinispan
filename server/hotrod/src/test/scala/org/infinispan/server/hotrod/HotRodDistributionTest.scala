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

import org.testng.annotations.Test
import org.infinispan.config.Configuration.CacheMode
import org.infinispan.config.Configuration
import java.lang.reflect.Method
import org.infinispan.server.hotrod.OperationStatus._
import test.HotRodTestingUtil._
import org.testng.Assert._
import org.infinispan.test.AbstractCacheTest._
import test.{TestHashDistAware10Response, HotRodClient}

// Do not remove, otherwise getDefaultClusteredConfig is not found
import scala.collection.JavaConversions._

/**
 * Tests Hot Rod logic when interacting with distributed caches, particularly logic to do with
 * hash-distribution-aware headers and how it behaves when cluster formation changes.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodDistributionTest")
class HotRodDistributionTest extends HotRodMultiNodeTest {

   override protected def cacheName: String = "hotRodDistSync"

   override protected def createCacheConfig: Configuration = {
      val cfg = getDefaultClusteredConfig(CacheMode.DIST_SYNC)
      cfg.fluent().l1().disable() // Disable L1 explicitly
      cfg
   }

   override protected def protocolVersion : Byte = 10

   def testDistributedPutWithTopologyChanges(m: Method) {
      val client1 = clients.head
      val client2 = clients.tail.head

      var resp = client1.ping(3, 0)
      assertStatus(resp, Success)
      assertHashTopology10Received(resp.topologyResponse.get, servers, cacheName)

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
      assertHashTopology10Received(resp.topologyResponse.get, servers, cacheName)
      assertSuccess(client2.get(k(m), 0), v(m, "v4-"))

      resp = client2.put(k(m) , 0, 0, v(m, "v5-"), 3, 0)
      assertStatus(resp, Success)
      assertHashTopology10Received(resp.topologyResponse.get, servers, cacheName)
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
         assertHashTopology10Received(resp.topologyResponse.get, allServers, cacheName)

         log.trace("Get key and verify that's v6-*")
         assertSuccess(client2.get(k(m), 0), v(m, "v6-"))

         resp = client2.put(k(m), 0, 0, v(m, "v7-"), 3, 0)
         assertStatus(resp, Success)
         assertHashTopology10Received(resp.topologyResponse.get, allServers, cacheName)

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
      assertHashTopology10Received(resp.topologyResponse.get, servers, cacheName)

      assertSuccess(client1.get(k(m), 0), v(m, "v8-"))
   }
}