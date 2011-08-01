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

import logging.Log
import OperationStatus._
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.Cache
import org.infinispan.server.core.CacheValue
import org.infinispan.util.ByteArrayKey
import scala.collection.JavaConversions._
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.Channel
import org.jboss.netty.buffer.ChannelBuffer
import org.infinispan.server.core.transport.ExtendedChannelBuffer._

/**
 * Hot Rod specific encoder.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class HotRodEncoder(cacheManager: EmbeddedCacheManager) extends OneToOneEncoder {
   import HotRodEncoder._
   import HotRodServer._

   private lazy val isClustered: Boolean = cacheManager.getGlobalConfiguration.getTransportClass != null
   private lazy val topologyCache: Cache[String, TopologyView] =
      if (isClustered) cacheManager.getCache(TopologyCacheName) else null

   override def encode(ctx: ChannelHandlerContext, ch: Channel, msg: AnyRef): AnyRef = {
      val isTrace = isTraceEnabled

      if (isTrace) trace("Encode msg %s", msg)
      val buffer: ChannelBuffer = msg match { 
         case r: Response => writeHeader(r, isTrace, getTopologyResponse(r))
      }
      msg match {
         case r: ResponseWithPrevious => {
            if (r.previous == None)
               writeUnsignedInt(0, buffer)
            else
               writeRangedBytes(r.previous.get, buffer)
         }
         case s: StatsResponse => {
            writeUnsignedInt(s.stats.size, buffer)
            for ((key, value) <- s.stats) {
               writeString(key, buffer)
               writeString(value, buffer)
            }
         }
         case g: GetWithVersionResponse => {
            if (g.status == Success) {
               buffer.writeLong(g.version)
               writeRangedBytes(g.data.get, buffer)
            }
         }
         case g: BulkGetResponse => {
            if (isTrace) trace("About to respond to bulk get request")
            if (g.status == Success) {
               val cache: Cache[ByteArrayKey, CacheValue] = getCacheInstance(g.cacheName, cacheManager)
               var iterator = asScalaIterator(cache.entrySet.iterator)
               if (g.count != 0) {
                  if (isTrace) trace("About to write (max) %d messages to the client", g.count)
                  iterator = iterator.take(g.count)
               }
               for (entry <- iterator) {
                  buffer.writeByte(1) // Not done
                  writeRangedBytes(entry.getKey.getData, buffer)
                  writeRangedBytes(entry.getValue.data, buffer)
               }
               buffer.writeByte(0) // Done
            }
         }
         case g: GetResponse => if (g.status == Success) writeRangedBytes(g.data.get, buffer)
         case e: ErrorResponse => writeString(e.msg, buffer)
         case _ => if (buffer == null) throw new IllegalArgumentException("Response received is unknown: " + msg);         
      }
      buffer
   }

   val DEFAULT_HASH_FUNCTION_VERSION: Byte = 2

   private def getTopologyResponse(r: Response): AbstractTopologyResponse = {
      // If clustered, set up a cache for topology information
      if (isClustered) {
         r.clientIntel match {
            case 2 | 3 => {
               val currentTopologyView = topologyCache.get("view")
               if (r.topologyId != currentTopologyView.topologyId) {
                  val cache = getCacheInstance(r.cacheName, cacheManager)
                  val config = cache.getConfiguration
                  if (r.clientIntel == 2 || !config.getCacheMode.isDistributed) {
                     TopologyAwareResponse(TopologyView(currentTopologyView.topologyId, currentTopologyView.members))
                  } else { // Must be 3 and distributed
                     // TODO: Retrieve hash function when we have specified functions
                     HashDistAwareResponse(TopologyView(currentTopologyView.topologyId, currentTopologyView.members),
                           config.getNumOwners, DEFAULT_HASH_FUNCTION_VERSION, Integer.MAX_VALUE)
                  }
               } else null
            }
            case 1 => null
         }
      } else null
   }

   private def writeHeader(r: Response, isTrace: Boolean, topologyResp: AbstractTopologyResponse): ChannelBuffer = {
      val buffer = dynamicBuffer
      buffer.writeByte(Magic.byteValue)
      writeUnsignedLong(r.messageId, buffer)
      buffer.writeByte(r.operation.id.byteValue)
      buffer.writeByte(r.status.id.byteValue)
      if (topologyResp != null) {
         topologyResp match {
            case t: TopologyAwareResponse => {
               if (r.clientIntel == 2)
                  writeTopologyHeader(t, buffer, isTrace)
               else
                  writeHashTopologyHeader(t, buffer, isTrace)
            }
            case h: HashDistAwareResponse => writeHashTopologyHeader(h, buffer, r, isTrace)
         }
      } else {
         buffer.writeByte(0) // No topology change
      }
      buffer
   }

   private def writeTopologyHeader(t: TopologyAwareResponse, buffer: ChannelBuffer, isTrace: Boolean) {
      if (isTrace) trace("Write topology change response header %s", t)
      buffer.writeByte(1) // Topology changed
      writeUnsignedInt(t.view.topologyId, buffer)
      writeUnsignedInt(t.view.members.size, buffer)
      t.view.members.foreach{address =>
         writeString(address.host, buffer)
         writeUnsignedShort(address.port, buffer)
      }
   }

   private def writeHashTopologyHeader(t: TopologyAwareResponse, buffer: ChannelBuffer, isTrace: Boolean) {
      if (isTrace) trace("Return limited hash distribution aware header in spite of having a hash aware client %s", t)
      buffer.writeByte(1) // Topology changed
      writeUnsignedInt(t.view.topologyId, buffer)
      writeUnsignedShort(0, buffer) // Num key owners
      buffer.writeByte(0) // Hash function
      writeUnsignedInt(0, buffer) // Hash space
      writeUnsignedInt(t.view.members.size, buffer)
      t.view.members.foreach{address =>
         writeString(address.host, buffer)
         writeUnsignedShort(address.port, buffer)
         buffer.writeInt(0) // Address' hash id
      }
   }

   private def writeHashTopologyHeader(h: HashDistAwareResponse, buffer: ChannelBuffer, r: Response, isTrace: Boolean) {
      if (isTrace) trace("Write hash distribution change response header %s", h)
      buffer.writeByte(1) // Topology changed
      writeUnsignedInt(h.view.topologyId, buffer)
      writeUnsignedShort(h.numOwners, buffer) // Num key owners
      buffer.writeByte(h.hashFunction) // Hash function
      writeUnsignedInt(h.hashSpace, buffer) // Hash space
      val numVNodes = getCacheInstance(r.cacheName, cacheManager).getConfiguration.getNumVirtualNodes
      // If virtual nodes are enabled, we need to send as many hashes as
      // cluster members * num virtual nodes. Otherwise, rely on the default
      // when virtual nodes is disabled which is '1'.
      val totalNumServers = h.view.members.size * numVNodes
      writeUnsignedInt(totalNumServers, buffer)
      h.view.members.foreach {address =>
         // Take hash ids associated with the cache
         val cacheHashIds = address.hashIds.get(r.cacheName).get
         // For each of the hash ids associated with this address and cache
         // (i.e. in case of virtual nodes), write an entry back the client.
         cacheHashIds.foreach { hashId =>
            writeString(address.host, buffer)
            writeUnsignedShort(address.port, buffer)
            buffer.writeInt(hashId)
         }
      }
   }

}

object HotRodEncoder extends Log {
   private val Magic = 0xA1
}
