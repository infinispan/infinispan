/*
 * Copyright 2013 Red Hat, Inc. and/or its affiliates.
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
import org.infinispan.util.ByteArrayEquivalence

/**
 * Tests Hot Rod with a local (non-clustered) cache using Hot Rod's 1.1 protocol.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRod11LocalCacheTest")
class HotRod11LocalCacheTest extends HotRodMultiNodeTest {

   override protected def cacheName = "localVersion11"

   override protected def createCacheConfig: ConfigurationBuilder =
      hotRodCacheConfiguration(
         getDefaultClusteredCacheConfig(CacheMode.LOCAL, false))

   override protected def protocolVersion : Byte = 11

   protected def virtualNodes = 1

   def testDistributedPutWithTopologyChanges(m: Method) {
      val client1 = clients.head
      val client2 = clients.tail.head

      var resp = client1.ping(INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)

      // Client intelligence is now 1, which means no topology updates
      resp = client1.put(k(m) , 0, 0, v(m), INTELLIGENCE_BASIC, 0)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)
      assertKeyDoesNotExist(client2.get(k(m), 0))

      resp = client2.put(k(m) , 0, 0, v(m, "v1-"), INTELLIGENCE_TOPOLOGY_AWARE, 0)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)
      // check that client1 still has the old value
      assertSuccess(client1.get(k(m), 0), v(m))

      val newServer = startClusteredServer(servers.tail.head.getPort + 25)
      val newClient = new HotRodClient(
            "127.0.0.1", newServer.getPort, cacheName, 60, protocolVersion)
      val allServers = newServer :: servers
      try {
         log.trace("New client started, modify key to be v6-*")
         resp = newClient.put(k(m) , 0, 0, v(m, "v2-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0)
         assertStatus(resp, Success)
         assertEquals(resp.topologyResponse, None)

         log.trace("Get key from the other clients and verify that it hasn't changed")
         assertSuccess(client1.get(k(m), 0), v(m))
         assertSuccess(client2.get(k(m), 0), v(m, "v1-"))

         resp = client2.put(k(m), 0, 0, v(m, "v3-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0)
         assertStatus(resp, Success)
         assertEquals(resp.topologyResponse, None)

         // now the new client's value doesn't change
         assertSuccess(newClient.get(k(m), 0), v(m, "v2-"))
      } finally {
         log.trace("Stopping new server")
         killClient(newClient)
         stopClusteredServer(newServer)
         log.trace("New server stopped")
      }

      resp = client2.put(k(m) , 0, 0, v(m, "v4-"), 3, 2)
      assertStatus(resp, Success)
      assertEquals(resp.topologyResponse, None)

      assertSuccess(client1.get(k(m), 0), v(m))
      assertSuccess(client2.get(k(m), 0), v(m, "v4-"))
   }


}