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

import logging.Log
import org.jboss.netty.buffer.ChannelBuffer
import org.infinispan.Cache
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.remoting.transport.Address
import org.infinispan.server.core.transport.ExtendedChannelBuffer._
import collection.JavaConversions._
import OperationStatus._
import org.infinispan.util.ByteArrayKey
import org.infinispan.server.core.CacheValue
import org.infinispan.configuration.cache.Configuration

/**
 * Hot Rod encoder for protocol version 1.1
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
abstract class AbstractEncoder1x extends AbstractVersionedEncoder with Constants with Log {

   import HotRodServer._

   override def writeHeader(r: Response, buf: ChannelBuffer,
           addressCache: Cache[Address, ServerAddress], server: HotRodServer) {
      val topologyResp = getTopologyResponse(r, addressCache, server)
      buf.writeByte(MAGIC_RES.byteValue)
      writeUnsignedLong(r.messageId, buf)
      buf.writeByte(r.operation.id.byteValue)
      buf.writeByte(r.status.id.byteValue)
      if (topologyResp != null) {
         topologyResp match {
            case t: TopologyAwareResponse => {
               if (r.clientIntel == 2)
                  writeTopologyHeader(t, buf, addressCache)
               else
                  writeHashTopologyHeader(t, buf, r, addressCache, server)
            }
            case h: AbstractHashDistAwareResponse =>
               writeHashTopologyHeader(h, buf, r, addressCache, server)
         }
      } else {
         buf.writeByte(0) // No topology change
      }
   }

   override def writeResponse(r: Response, buf: ChannelBuffer,
           cacheManager: EmbeddedCacheManager, server: HotRodServer) {
      r match {
         case r: ResponseWithPrevious => {
            if (r.previous == None)
               writeUnsignedInt(0, buf)
            else
               writeRangedBytes(r.previous.get, buf)
         }
         case s: StatsResponse => {
            writeUnsignedInt(s.stats.size, buf)
            for ((key, value) <- s.stats) {
               writeString(key, buf)
               writeString(value, buf)
            }
         }
         case g: GetWithVersionResponse => {
            if (g.status == Success) {
               buf.writeLong(g.dataVersion)
               writeRangedBytes(g.data.get, buf)
            }
         }
         case g: BulkGetResponse => {
            log.trace("About to respond to bulk get request")
            if (g.status == Success) {
               val cache: Cache[ByteArrayKey, CacheValue] =
                  server.getCacheInstance(g.cacheName, cacheManager, false)
               var iterator = asScalaIterator(cache.entrySet.iterator)
               if (g.count != 0) {
                  trace("About to write (max) %d messages to the client", g.count)
                  iterator = iterator.take(g.count)
               }
               for (entry <- iterator) {
                  buf.writeByte(1) // Not done
                  writeRangedBytes(entry.getKey.getData, buf)
                  writeRangedBytes(entry.getValue.data, buf)
               }
               buf.writeByte(0) // Done
            }
         }
         case g: GetResponse =>
            if (g.status == Success) writeRangedBytes(g.data.get, buf)
         case e: ErrorResponse => writeString(e.msg, buf)
         case _ => if (buf == null)
            throw new IllegalArgumentException("Response received is unknown: " + r);
      }
   }

   def getTopologyResponse(r: Response, addressCache: Cache[Address, ServerAddress],
           server: HotRodServer): AbstractTopologyResponse = {
      // If clustered, set up a cache for topology information
      if (addressCache != null) {
         r.clientIntel match {
            case 2 | 3 => {
               val lastViewId = server.getViewId
               // Topology is only considered to be outdated when it's older
               // (smaller value), than the one sent by the client. If the
               // comparison was only done on whether the view id was different,
               // it could result in receiving old topologies and potentially
               // trying to connect to servers that are down.
               // Besides, make sure that a view has actually been set! In other
               // words, check against default value to see if it's higher.
               if (lastViewId >= DEFAULT_VIEW_ID && r.topologyId < lastViewId)
                  generateTopologyResponse(r, addressCache, server, lastViewId)
               else null
            }
            case 1 => null
         }
      } else null
   }

   private def generateTopologyResponse(r: Response, addressCache: Cache[Address, ServerAddress],
           server: HotRodServer, lastViewId: Int): AbstractTopologyResponse = {
      val cache = server.getCacheInstance(r.cacheName, addressCache.getCacheManager, false)
      val config = cache.getCacheConfiguration
      if (r.clientIntel == 2 || !config.clustering().cacheMode().isDistributed) {
         TopologyAwareResponse(lastViewId)
      } else {
         // Must be 3 and distributed
         createHashDistAwareResp(lastViewId, config)
      }
   }

   protected def createHashDistAwareResp(lastViewId: Int, cfg: Configuration): AbstractHashDistAwareResponse = {
      HashDistAwareResponse(lastViewId, cfg.clustering().hash().numOwners(),
         DEFAULT_HASH_FUNCTION_VERSION, Integer.MAX_VALUE)
   }

   def writeTopologyHeader(t: TopologyAwareResponse,
           buffer: ChannelBuffer, addrCache: Cache[Address, ServerAddress]) {
      trace("Write topology change response header %s", t)
      buffer.writeByte(1) // Topology changed
      writeUnsignedInt(t.viewId, buffer)
      val serverAddresses = addrCache.values()
      writeUnsignedInt(serverAddresses.size, buffer)
      serverAddresses.foreach{address =>
         writeString(address.host, buffer)
         writeUnsignedShort(address.port, buffer)
      }
   }

   protected def writeHashTopologyHeader(topoRsp: AbstractTopologyResponse,
           buffer: ChannelBuffer, r: Response, members: Cache[Address, ServerAddress],
           server: HotRodServer) {
      topoRsp match {
         case h: AbstractHashDistAwareResponse => {
            trace("Write hash distribution change response header %s", h)
            // If virtual nodes are enabled, we need to send as many hashes as
            // cluster members * num virtual nodes. Otherwise, rely on the default
            // when virtual nodes is disabled which is '1'.
            val cache = server.getCacheInstance(r.cacheName, members.getCacheManager, false)
            val numVNodes = cache.getCacheConfiguration.clustering().hash().numVirtualNodes()

            val clusterMembers = members.getCacheManager.getMembers
            val totalNumServers = clusterMembers.size * numVNodes
            writeCommonHashTopologyHeader(buffer, h.viewId, h.numOwners, h.hashFunction,
               h.hashSpace, totalNumServers)

            // This is not performant at all, but the idea here is to be able to be
            // consistent with the version 1.0 of the protocol. With time, users
            // should migrate to version 1.1 capable clients.
            val distManager = cache.getAdvancedCache.getDistributionManager
            clusterMembers.foreach {clusterAddr =>
            // Take hash ids associated with the cache
               val cacheHashIds = distManager.getConsistentHash.getHashIds(clusterAddr)
               val address = members.get(clusterAddr)
               // For each of the hash ids associated with this address and cache
               // (i.e. in case of virtual nodes), write an entry back the client.
               cacheHashIds.foreach { hashId =>
                  writeString(address.host, buffer)
                  writeUnsignedShort(address.port, buffer)
                  buffer.writeInt(hashId)
               }
            }
         }
         case t: TopologyAwareResponse => {
            trace("Return limited hash distribution aware header in spite of having a hash aware client %s", t)
            val serverAddresses = members.values()
            writeCommonHashTopologyHeader(buffer, t.viewId, 0, 0, 0, serverAddresses.size)
            serverAddresses.foreach { address =>
               writeString(address.host, buffer)
               writeUnsignedShort(address.port, buffer)
               buffer.writeInt(0) // Address' hash id
            }
         }
      }
   }

   protected def writeCommonHashTopologyHeader(buffer: ChannelBuffer, viewId: Int,
           numOwners: Int, hashFct: Byte, hashSpace: Int, numServers: Int) {
      buffer.writeByte(1) // Topology changed
      writeUnsignedInt(viewId, buffer)
      writeUnsignedShort(numOwners, buffer) // Num key owners
      buffer.writeByte(hashFct) // Hash function
      writeUnsignedInt(hashSpace, buffer) // Hash space
      writeUnsignedInt(numServers, buffer)
   }

}