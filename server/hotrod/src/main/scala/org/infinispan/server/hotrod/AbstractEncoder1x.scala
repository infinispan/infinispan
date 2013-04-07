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
import org.infinispan.configuration.cache.Configuration
import org.infinispan.distribution.ch.DefaultConsistentHash
import collection.mutable.ArrayBuffer
import org.infinispan.server.hotrod.util.BulkUtil

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
               if (r.clientIntel == INTELLIGENCE_TOPOLOGY_AWARE)
                  writeTopologyUpdate(t, buf)
               else
                  writeLimitedHashTopologyUpdate(t, buf)
            }
            case h: AbstractHashDistAwareResponse =>
               writeHashTopologyUpdate(h, server, r, buf)
         }
      } else {
         writeNoTopologyUpdate(buf)
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
         case g: GetWithMetadataResponse => {
            if (g.status == Success) {
               val flags = (if (g.lifespan < 0) INFINITE_LIFESPAN else 0) + (if (g.maxIdle < 0 ) INFINITE_MAXIDLE else 0)
               buf.writeByte(flags)
               if (g.lifespan >= 0) {
                  buf.writeLong(g.created)
                  writeUnsignedInt(g.lifespan, buf)
               }
               if (g.maxIdle >= 0) {
                  buf.writeLong(g.lastUsed)
                  writeUnsignedInt(g.maxIdle, buf)
               }
               buf.writeLong(g.dataVersion)
               writeRangedBytes(g.data.get, buf)
            }
         }
         case g: BulkGetResponse => {
            log.trace("About to respond to bulk get request")
            if (g.status == Success) {
               val cache: Cache[Array[Byte], Array[Byte]] =
                  server.getCacheInstance(g.cacheName, cacheManager, false)
               var iterator = asScalaIterator(cache.entrySet.iterator)
               if (g.count != 0) {
                  trace("About to write (max) %d messages to the client", g.count)
                  iterator = iterator.take(g.count)
               }
               for (entry <- iterator) {
                  buf.writeByte(1) // Not done
                  writeRangedBytes(entry.getKey, buf)
                  writeRangedBytes(entry.getValue, buf)
               }
               buf.writeByte(0) // Done
            }
         }
         case g: BulkGetKeysResponse => {
         	log.trace("About to respond to bulk get keys request")
            if (g.status == Success) {
               val cache: Cache[Array[Byte], Array[Byte]] =
                  server.getCacheInstance(g.cacheName, cacheManager, false)
               
               var keys = BulkUtil.getAllKeys(cache, g.scope)
               var iterator = asScalaIterator(keys.iterator)
               for (key <- iterator) {
                  buf.writeByte(1) // Not done
                  writeRangedBytes(key, buf)
               }
               buf.writeByte(0) // Done
            }
         }
         case g: GetResponse =>
            if (g.status == Success) writeRangedBytes(g.data.get, buf)
         case e: ErrorResponse => writeString(e.msg, buf)
         case _ => if (buf == null)
            throw new IllegalArgumentException("Response received is unknown: " + r)
      }
   }

   def getTopologyResponse(r: Response, addressCache: Cache[Address, ServerAddress],
           server: HotRodServer): AbstractTopologyResponse = {
      // If clustered, set up a cache for topology information
      if (addressCache != null) {
         r.clientIntel match {
            case INTELLIGENCE_TOPOLOGY_AWARE | INTELLIGENCE_HASH_DISTRIBUTION_AWARE => {
               // Use the request cache's topology id as the HotRod topologyId.
               val cache = server.getCacheInstance(r.cacheName, addressCache.getCacheManager, false)
               val rpcManager = cache.getAdvancedCache.getRpcManager
               // Only send a topology update if the cache is clustered
               val currentTopologyId = rpcManager match {
                  case null => DEFAULT_TOPOLOGY_ID
                  case _ => rpcManager.getTopologyId
               }
               // AND if the client's topology id is smaller than the server's topology id
               if (currentTopologyId >= DEFAULT_TOPOLOGY_ID && r.topologyId < currentTopologyId)
                  generateTopologyResponse(r, addressCache, server, currentTopologyId)
               else null
            }
            case INTELLIGENCE_BASIC => null
         }
      } else null
   }

   private def generateTopologyResponse(r: Response, addressCache: Cache[Address, ServerAddress],
           server: HotRodServer, currentTopologyId: Int): AbstractTopologyResponse = {
      // If the topology cache is incomplete, we assume that a node has joined but hasn't added his HotRod
      // endpoint address to the topology cache yet. We delay the topology update until the next client
      // request by returning null here (so the client topology id stays the same).
      // If a new client connects while the join is in progress, though, we still have to generate a topology
      // response. Same if we have cache manager that is a member of the cluster but doesn't have a HotRod
      // endpoint (aka a storage-only node), and a HotRod server shuts down.
      // Our workaround is to send a "partial" topology update when the topology cache is incomplete, but the
      // difference between the client topology id and the server topology id is 2 or more. The partial update
      // will have the topology id of the server - 1, so it won't prevent a regular topology update if/when
      // the topology cache is updated.
      val cache = server.getCacheInstance(r.cacheName, addressCache.getCacheManager, false)
      val cacheMembers = cache.getAdvancedCache.getRpcManager.getMembers
      val serverEndpointsMap = addressCache.toMap
      var responseTopologyId = currentTopologyId
      if (!serverEndpointsMap.keySet.containsAll(cacheMembers)) {
         // At least one cache member is missing from the topology cache
         val clientTopologyId = r.topologyId
         if (currentTopologyId - clientTopologyId < 2) {
            // Postpone topology update
            return null
         } else {
            // Send partial topology update
            responseTopologyId -= 1
         }
      }

      val config = cache.getCacheConfiguration
      if (r.clientIntel == INTELLIGENCE_TOPOLOGY_AWARE || !config.clustering().cacheMode().isDistributed) {
         TopologyAwareResponse(responseTopologyId, serverEndpointsMap)
      } else {
         // Must be 3 and distributed
         createHashDistAwareResp(responseTopologyId, serverEndpointsMap, config)
      }
   }

   protected def createHashDistAwareResp(topologyId: Int, serverEndpointsMap: Map[Address, ServerAddress],
                                         cfg: Configuration): AbstractHashDistAwareResponse = {
      HashDistAwareResponse(topologyId, serverEndpointsMap, cfg.clustering().hash().numOwners(),
         DEFAULT_HASH_FUNCTION_VERSION, Integer.MAX_VALUE)
   }

   def writeHashTopologyUpdate(h: AbstractHashDistAwareResponse, server: HotRodServer, r: Response,
                               buffer: ChannelBuffer) {
      trace("Write hash distribution change response header %s", h)
      val cache = server.getCacheInstance(r.cacheName, server.getCacheManager, false)
      val distManager = cache.getAdvancedCache.getDistributionManager
      val ch = distManager.getConsistentHash

      // This is not quite correct, as the ownership of segments on the 1.0/1.1/1.2 clients is not exactly
      // the same as on the server. But the difference appears only for (numSegment*numOwners/MAX_INT)
      // of the keys (at the "segment borders"), so it's still much better than having no hash information.
      // The idea here is to be able to be compatible with clients running version 1.0 of the protocol.
      // TODO Need a check somewhere on startup, this only works with the default consistent hash
      val numSegments = ch.getNumSegments
      val segmentHashIds = ch.asInstanceOf[DefaultConsistentHash].getSegmentEndHashes
      val serverHashes = ArrayBuffer[(ServerAddress, Int)]()
      for ((address, serverAddress) <- h.serverEndpointsMap) {
         for (segmentIdx <- 0 until numSegments) {
            val ownerIdx = ch.locateOwnersForSegment(segmentIdx).indexOf(address)
            if (ownerIdx >= 0) {
               val segmentHashId = segmentHashIds(segmentIdx)
               val hashId = (segmentHashId + ownerIdx) & Int.MaxValue
               serverHashes += ((serverAddress, hashId))
            }
         }
      }

      val totalNumServers = serverHashes.size
      writeCommonHashTopologyHeader(buffer, h.topologyId, h.numOwners, h.hashFunction,
         h.hashSpace, totalNumServers)
      for ((serverAddress, hashId) <- serverHashes) {
         writeString(serverAddress.host, buffer)
         writeUnsignedShort(serverAddress.port, buffer)
         log.tracef("Writing hash id %d for %s:%s", hashId, serverAddress.host, serverAddress.port)
         buffer.writeInt(hashId)
      }
   }

   def writeLimitedHashTopologyUpdate(t: AbstractTopologyResponse, buffer: ChannelBuffer) {
      trace("Return limited hash distribution aware header because the client %s doesn't ", t)
      writeCommonHashTopologyHeader(buffer, t.topologyId, 0, 0, 0, t.serverEndpointsMap.size)
      for (address <- t.serverEndpointsMap.values) {
         writeString(address.host, buffer)
         writeUnsignedShort(address.port, buffer)
         buffer.writeInt(0) // Address' hash id
      }
   }

   def writeTopologyUpdate(t: TopologyAwareResponse, buffer: ChannelBuffer) {
      trace("Write topology change response header %s", t)
      buffer.writeByte(1) // Topology changed
      writeUnsignedInt(t.topologyId, buffer)
      writeUnsignedInt(t.serverEndpointsMap.size, buffer)
      for (address <- t.serverEndpointsMap.values) {
         writeString(address.host, buffer)
         writeUnsignedShort(address.port, buffer)
      }
   }


   def writeNoTopologyUpdate(buffer: ChannelBuffer) {
      trace("Write topology response header with no change")
      buffer.writeByte(0)
   }

   protected def writeCommonHashTopologyHeader(buffer: ChannelBuffer, viewId: Int,
           numOwners: Int, hashFct: Byte, hashSpace: Int, numServers: Int) {
      buffer.writeByte(1) // Topology changed
      writeUnsignedInt(viewId, buffer)
      writeUnsignedShort(numOwners, buffer) // Num key owners
      buffer.writeByte(hashFct) // Hash function
      writeUnsignedInt(hashSpace, buffer) // Hash space
      writeUnsignedInt(numServers, buffer)
      trace("Topology will contain %d addresses", numServers)
   }

}
