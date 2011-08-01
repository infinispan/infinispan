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
import test.HotRodClient
import test.HotRodTestingUtil._
import org.testng.Assert._
import org.infinispan.test.TestingUtil
import org.infinispan.distribution.ch.UnionConsistentHash
import collection.mutable.ListBuffer
import org.infinispan.test.AbstractCacheTest._ // Do not remove, otherwise getDefaultClusteredConfig is not found
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

   override protected def createCacheConfig: Configuration = getDefaultClusteredConfig(CacheMode.DIST_SYNC)

   def testDistributedPutWithTopologyChanges(m: Method) {
      var resp = clients.head.ping(3, 0)
      assertStatus(resp, Success)
      var expectedHashIds = generateExpectedHashIds
      assertHashTopologyReceived(resp.topologyResponse.get, servers, expectedHashIds)

      resp = clients.head.put(k(m) , 0, 0, v(m), 1, 0)
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

      resp = clients.head.put(k(m) , 0, 0, v(m, "v4-"), 3, 0)
      assertStatus(resp, Success)
      expectedHashIds = generateExpectedHashIds
      assertHashTopologyReceived(resp.topologyResponse.get, servers, expectedHashIds)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v4-"))
      resp = clients.tail.head.put(k(m) , 0, 0, v(m, "v5-"), 3, 1)
      assertStatus(resp, Success)
      expectedHashIds = generateExpectedHashIds
      assertHashTopologyReceived(resp.topologyResponse.get, servers, expectedHashIds)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v5-"))

      val newServer = startClusteredServer(servers.tail.head.getPort + 25)
      val newClient = new HotRodClient("127.0.0.1", newServer.getPort, cacheName, 60)
      try {
         log.trace("New client started, modify key to be v6-*")
         resp = newClient.put(k(m) , 0, 0, v(m, "v6-"), 3, 2)
         // resp = clients.tail.head.put(k(m) , 0, 0, v(m, "v6-"), 3, 2)
         assertStatus(resp, Success)
         val hashTopologyResp = resp.topologyResponse.get.asInstanceOf[HashDistAwareResponse]
         assertEquals(hashTopologyResp.view.topologyId, 3)
         assertEquals(hashTopologyResp.view.members.size, 3)
         val consistentHash = cacheManagers.get(2).getCache(cacheName).getAdvancedCache.getDistributionManager.getConsistentHash

         var ids = consistentHash.getHashIds(servers.head.getAddress.clusterAddress)
         assertAddressEquals(hashTopologyResp.view.members.head, servers.head.getAddress,
            Map(cacheName -> asScalaBuffer(ids.asInstanceOf[java.util.List[Int]]).toSeq))

         ids = consistentHash.getHashIds(servers.tail.head.getAddress.clusterAddress)
         assertAddressEquals(hashTopologyResp.view.members.tail.head, servers.tail.head.getAddress,
            Map(cacheName -> asScalaBuffer(ids.asInstanceOf[java.util.List[Int]]).toSeq))

         ids = consistentHash.getHashIds(newServer.getAddress.clusterAddress)
         assertAddressEquals(hashTopologyResp.view.members.tail.tail.head, newServer.getAddress,
            Map(cacheName -> asScalaBuffer(ids.asInstanceOf[java.util.List[Int]]).toSeq))

         assertEquals(hashTopologyResp.numOwners, 2)
         assertEquals(hashTopologyResp.hashFunction, EXPECTED_HASH_FUNCTION_VERSION)
         assertEquals(hashTopologyResp.hashSpace, Integer.MAX_VALUE)
         log.trace("Get key and verify that's v6-*")
         assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v6-"))
      } finally {
         newClient.stop
         stopClusteredServer(newServer)
      }

      resp = clients.tail.head.put(k(m) , 0, 0, v(m, "v7-"), 3, 3)
      assertStatus(resp, Success)
      val hashTopologyResp = resp.topologyResponse.get.asInstanceOf[HashDistAwareResponse]
      assertEquals(hashTopologyResp.view.topologyId, 4)
      assertEquals(hashTopologyResp.view.members.size, 2)
      val consistentHash = cacheManagers.get(1).getCache(cacheName).getAdvancedCache.getDistributionManager.getConsistentHash

      var ids = consistentHash.getHashIds(servers.head.getAddress.clusterAddress)
      assertAddressEquals(hashTopologyResp.view.members.head, servers.head.getAddress,
         Map(cacheName -> asScalaBuffer(ids.asInstanceOf[java.util.List[Int]]).toSeq))

      ids = consistentHash.getHashIds(servers.tail.head.getAddress.clusterAddress)
      assertAddressEquals(hashTopologyResp.view.members.tail.head, servers.tail.head.getAddress,
         Map(cacheName -> asScalaBuffer(ids.asInstanceOf[java.util.List[Int]]).toSeq))

      assertEquals(hashTopologyResp.numOwners, 2)
      assertEquals(hashTopologyResp.hashFunction, EXPECTED_HASH_FUNCTION_VERSION)
      assertEquals(hashTopologyResp.hashSpace, Integer.MAX_VALUE)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v7-"))
   }

   private def generateExpectedHashIds: List[Map[String, Seq[Int]]] = {
      val listBuffer = new ListBuffer[Map[String, Seq[Int]]]
      val consistentHash = cacheManagers.get(0).getCache(cacheName).getAdvancedCache.getDistributionManager.getConsistentHash
      var i = 0
      while (consistentHash.isInstanceOf[UnionConsistentHash] && i < 10) {
         TestingUtil.sleepThread(1000)
         i += 1
      }
      var ids = consistentHash.getHashIds(servers.head.getAddress.clusterAddress)
      listBuffer += Map(cacheName -> asScalaBuffer(ids.asInstanceOf[java.util.List[Int]]).toSeq)
      ids = consistentHash.getHashIds(servers.tail.head.getAddress.clusterAddress)
      listBuffer += Map(cacheName -> asScalaBuffer(ids.asInstanceOf[java.util.List[Int]]).toSeq)
      listBuffer.toList
   }
}