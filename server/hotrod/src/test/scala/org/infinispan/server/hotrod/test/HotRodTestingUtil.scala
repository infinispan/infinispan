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
package org.infinispan.server.hotrod.test

import java.util.concurrent.atomic.AtomicInteger
import java.lang.reflect.Method
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod._
import logging.Log
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.core.Main._
import java.util.{Properties, Arrays}
import org.infinispan.util.{TypedProperties, Util}
import org.testng.Assert._
import org.infinispan.notifications.Listener
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent
import org.infinispan.remoting.transport.Address
import java.util.concurrent.{TimeUnit, CountDownLatch}
import collection.mutable.{ArrayBuffer, ListBuffer}
import org.jboss.netty.channel.ChannelFuture
import org.infinispan.configuration.cache.ConfigurationBuilder
import scala.Byte
import org.infinispan.distribution.ch.ConsistentHash
import scala.collection.JavaConversions._
import org.infinispan.test.TestingUtil
import org.infinispan.statetransfer.StateTransferManager

/**
 * Test utils for Hot Rod tests.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
object HotRodTestingUtil extends Log {

   val EXPECTED_HASH_FUNCTION_VERSION: Byte = 2

   def host = "127.0.0.1"

   def startHotRodServer(manager: EmbeddedCacheManager): HotRodServer =
      startHotRodServer(manager, UniquePortThreadLocal.get.intValue)

   def startHotRodServer(manager: EmbeddedCacheManager, proxyHost: String, proxyPort: Int): HotRodServer =
      startHotRodServer(manager, UniquePortThreadLocal.get.intValue, 0, proxyHost, proxyPort)

   def startHotRodServer(manager: EmbeddedCacheManager, port: Int): HotRodServer =
      startHotRodServer(manager, port, 0)

   def startHotRodServer(manager: EmbeddedCacheManager, port:Int, proxyHost: String, proxyPort: Int): HotRodServer =
      startHotRodServer(manager, port, 0, proxyHost, proxyPort)

   def startHotRodServer(manager: EmbeddedCacheManager, port: Int, idleTimeout: Int): HotRodServer =
      startHotRodServer(manager, port, idleTimeout, host, port)

   def startHotRodServer(manager: EmbeddedCacheManager, port: Int, idleTimeout: Int, proxyHost: String, proxyPort: Int): HotRodServer =
      startHotRodServer(manager, port, idleTimeout, proxyHost, proxyPort, -1)

   def startHotRodServerWithDelay(manager: EmbeddedCacheManager, port: Int, delay: Long): HotRodServer =
      startHotRodServer(manager, port, 0, host, port, delay)

   def startHotRodServer(manager: EmbeddedCacheManager, port: Int, idleTimeout: Int,
                         proxyHost: String, proxyPort: Int, delay: Long): HotRodServer = {
      val properties = new Properties
      properties.setProperty(PROP_KEY_IDLE_TIMEOUT, idleTimeout.toString)
      properties.setProperty(PROP_KEY_PROXY_HOST, proxyHost)
      properties.setProperty(PROP_KEY_PROXY_PORT, proxyPort.toString)
      startHotRodServer(manager, port, delay, properties)
   }

   def startHotRodServer(manager: EmbeddedCacheManager, port: Int, props: Properties): HotRodServer =
      startHotRodServer(manager, port, 0, props)

   def startHotRodServer(manager: EmbeddedCacheManager, port: Int, delay: Long, props: Properties): HotRodServer = {
      info("Start server in port %d", port)
      val server = new HotRodServer {
         override protected def createTopologyCacheConfig(typedProps: TypedProperties, distSyncTimeout: Long): ConfigurationBuilder = {
            if (delay > 0)
               Thread.sleep(delay)

            val cfg = super.createTopologyCacheConfig(typedProps, distSyncTimeout)
            cfg.transaction().syncCommitPhase(false).syncRollbackPhase(false)
            cfg
         }
      }
      props.setProperty(PROP_KEY_HOST, host)
      props.setProperty(PROP_KEY_PORT, port.toString)
      server.start(props, manager)



      server
   }

   private def getProperties(host: String, port: Int, idleTimeout: Int, proxyHost: String, proxyPort: Int): Properties = {
      val properties = new Properties
      properties.setProperty(PROP_KEY_HOST, host)
      properties.setProperty(PROP_KEY_PORT, port.toString)
      properties.setProperty(PROP_KEY_IDLE_TIMEOUT, idleTimeout.toString)
      properties.setProperty(PROP_KEY_PROXY_HOST, proxyHost)
      properties.setProperty(PROP_KEY_PROXY_PORT, proxyPort.toString)
      properties
   }

   def startCrashingHotRodServer(manager: EmbeddedCacheManager, port: Int): HotRodServer = {
      val server = new HotRodServer {
         override protected def createTopologyCacheConfig(typedProps: TypedProperties, distSyncTimeout: Long): ConfigurationBuilder = {
            val cfg = super.createTopologyCacheConfig(typedProps, distSyncTimeout)
            cfg.transaction().syncCommitPhase(false).syncRollbackPhase(false)
            cfg
         }
      }
      server.start(getProperties(host, port, 0, host, port), manager)
      server
   }

   def k(m: Method, prefix: String): Array[Byte] = {
      val bytes: Array[Byte] = (prefix + m.getName).getBytes
      trace("String %s is converted to %s bytes", prefix + m.getName, Util.printArray(bytes, true))
      bytes
   }

   def v(m: Method, prefix: String): Array[Byte] = k(m, prefix)

   def k(m: Method): Array[Byte] = k(m, "k-")

   def v(m: Method): Array[Byte] = v(m, "v-")

   def assertStatus(resp: TestResponse, expected: OperationStatus): Boolean = {
      val status = resp.status
      val isSuccess = status == expected
      resp match {
         case e: TestErrorResponse =>
            assertTrue(isSuccess,
               "Status should have been '%s' but instead was: '%s', and the error message was: %s"
               .format(expected, status, e.msg))
         case _ => assertTrue(isSuccess,
               "Status should have been '%s' but instead was: '%s'"
               .format(expected, status))
      }
      isSuccess
   }

   def assertSuccess(resp: TestGetResponse, expected: Array[Byte]): Boolean = {
      assertStatus(resp, Success)
      val isArrayEquals = Arrays.equals(expected, resp.data.get)
      assertTrue(isArrayEquals, "Retrieved data should have contained " + Util.printArray(expected, true)
            + " (" + new String(expected) + "), but instead we received " + Util.printArray(resp.data.get, true) + " (" +  new String(resp.data.get) +")")
      isArrayEquals
   }

   def assertSuccess(resp: TestGetWithVersionResponse, expected: Array[Byte], expectedVersion: Int): Boolean = {
      assertTrue(resp.version != expectedVersion)
      assertSuccess(resp, expected)
   }

   def assertSuccess(resp: TestResponseWithPrevious, expected: Array[Byte]): Boolean = {
      assertStatus(resp, Success)
      val isSuccess = Arrays.equals(expected, resp.previous.get)
      assertTrue(isSuccess)
      isSuccess
   }

   def assertKeyDoesNotExist(resp: TestGetResponse): Boolean = {
      val status = resp.status
      assertTrue(status == KeyDoesNotExist, "Status should have been 'KeyDoesNotExist' but instead was: " + status)
      assertEquals(resp.data, None)
      status == KeyDoesNotExist
   }

   def assertTopologyReceived(resp: AbstractTopologyResponse, servers: List[HotRodServer]) {
      assertTopologyId(resp.viewId, servers.head.getCacheManager)
      resp match {
         case t: TestTopologyAwareResponse =>
            assertEquals(t.members.size, 2)
            t.members.foreach(member => servers.map(_.getAddress).exists(_ == member))
         case h10: TestHashDistAware10Response =>
            assertEquals(h10.members.size, 2)
            h10.members.foreach(member => servers.map(_.getAddress).exists(_ == member))
         case h11: TestHashDistAware11Response =>
            assertEquals(h11.membersToHash.size, 2)
            h11.membersToHash.foreach(member => servers.map(_.getAddress).exists(_ == member))
      }
   }

   def assertHashTopology10Received(topoResp: AbstractTopologyResponse, servers: List[HotRodServer],
                                  cacheName: String) {
      assertHashTopology10Received(topoResp, servers, cacheName, 2,
            EXPECTED_HASH_FUNCTION_VERSION, Integer.MAX_VALUE)
   }

   def assertNoHashTopologyReceived(topoResp: AbstractTopologyResponse, servers: List[HotRodServer],
                                    cacheName: String) {
      assertHashTopology10Received(topoResp, servers, cacheName, 0, 0, 0)
   }

   def assertHashTopology10Received(topoResp: AbstractTopologyResponse,
                                    servers: List[HotRodServer], cacheName: String,
                                    expectedNumOwners: Int, expectedHashFct: Int, expectedHashSpace: Int) {
      val hashTopologyResp = topoResp.asInstanceOf[TestHashDistAware10Response]
      assertTopologyId(hashTopologyResp.viewId, servers.head.getCacheManager)
      assertEquals(hashTopologyResp.members.size, servers.size)
      hashTopologyResp.members.foreach(member => servers.map(_.getAddress).exists(_ == member))
      assertEquals(hashTopologyResp.numOwners, expectedNumOwners)
      assertEquals(hashTopologyResp.hashFunction, expectedHashFct)
      assertEquals(hashTopologyResp.hashSpace, expectedHashSpace)
      assertHashIds(hashTopologyResp.hashIds, servers, cacheName)
   }


   def assertHashTopologyReceived(topoResp: AbstractTopologyResponse,
                                  servers: List[HotRodServer], cacheName: String,
                                  expectedNumOwners: Int, expectedVirtualNodes: Int) {
      val hashTopologyResp = topoResp.asInstanceOf[TestHashDistAware11Response]
      assertTopologyId(hashTopologyResp.viewId, servers.head.getCacheManager)
      assertEquals(hashTopologyResp.membersToHash.size, servers.size)
      hashTopologyResp.membersToHash.foreach(member => servers.map(_.getAddress).exists(_ == member))
      assertEquals(hashTopologyResp.numOwners, expectedNumOwners)
      assertEquals(hashTopologyResp.hashFunction, EXPECTED_HASH_FUNCTION_VERSION)
      assertEquals(hashTopologyResp.hashSpace, Integer.MAX_VALUE)
      assertEquals(hashTopologyResp.numVirtualNodes, expectedVirtualNodes)
   }

   def assertHashIds(hashIds: Map[ServerAddress, Seq[Int]], servers: List[HotRodServer], cacheName: String) {
      val cache = servers.head.getCacheManager.getCache(cacheName)
      val stateTransferManager = TestingUtil.extractComponent(cache, classOf[StateTransferManager])
      val consistentHash = stateTransferManager.getCacheTopology.getCurrentCH
      val numSegments = consistentHash.getNumSegments
      val numOwners = consistentHash.getNumOwners
      assert(hashIds.size == servers.size)

      val segmentSize = math.ceil(Int.MaxValue.toDouble / numSegments).toInt
      val owners = new Array[ArrayBuffer[ServerAddress]](numSegments)

      for ((serverAddress, serverHashIds) <- hashIds) {
         for (hashId <- serverHashIds) {
            val segmentIdx = hashId / segmentSize
            if (owners(segmentIdx) == null) {
               owners(segmentIdx) = new ArrayBuffer[ServerAddress]
            }
            owners(segmentIdx) += serverAddress
         }
      }

      for (i <- 0 until numSegments) {
         val segmentOwners = owners(i)
         assert(segmentOwners.size == numOwners)
         val uniqueOwners = segmentOwners.toSet
         val chOwners = consistentHash.locateOwnersForSegment(i)
               .map(a => clusterAddressToServerAddress(servers, a)).toSet
         assert(uniqueOwners == chOwners)
      }
   }

   def clusterAddressToServerAddress(servers : List[HotRodServer], clusterAddress : Address) : ServerAddress = {
      return servers.find(_.getCacheManager.getAddress == clusterAddress).get.getAddress
   }

   def assertTopologyId(viewId: Int, cm: EmbeddedCacheManager) {
      assertEquals(viewId, cm.getCache(HotRodServer.ADDRESS_CACHE_NAME)
              .getAdvancedCache.getRpcManager.getTransport.getViewId)
   }

   def getAddressCacheRemovalLatches(servers: List[HotRodServer]): Seq[CountDownLatch] = {
      val latches = new ListBuffer[CountDownLatch]
      servers.foreach { server =>
         val addressRemovalLatch = new CountDownLatch(1)
         server.getAddressCache.addListener(
            new AddressRemovalListener(addressRemovalLatch))
         latches += addressRemovalLatch
      }
      latches.toList
   }

   def waitAddressCacheRemoval(latches: Seq[CountDownLatch]) {
      latches.foreach { latch =>
         val completed = latch.await(60, TimeUnit.SECONDS)
         if (!completed)
          throw new Exception("Timed out waiting for address cache to be updated")
      }
   }

   def killClient(client: HotRodClient): ChannelFuture = {
      try {
         if (client != null) client.stop
         else null
      }
      catch {
         case t: Throwable => {
            error("Error stopping client", t)
            null
         }
      }
   }

   @Listener
   private class AddressRemovalListener(latch: CountDownLatch) {

      @CacheEntryRemoved
      def addressRemoved(event: CacheEntryRemovedEvent[Address, ServerAddress]) {
         if (!event.isPre) // Only count down latch after address has been removed
            latch.countDown()
      }

   }

} 

object UniquePortThreadLocal extends ThreadLocal[Int] {

   private val uniqueAddr = new AtomicInteger(12311)

   override def initialValue: Int = {
      debug("Before incrementing, server port is: %d", uniqueAddr.get())
      val port = uniqueAddr.getAndAdd(100)
      debug("For next thread, server port will be: %d", uniqueAddr.get())
      port
   }

}
