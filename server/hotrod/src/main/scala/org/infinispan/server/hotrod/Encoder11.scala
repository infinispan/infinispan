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
import org.infinispan.server.hotrod.HotRodServer._
import collection.JavaConversions._

/**
 * Hot Rod encoder for protocol version 1.1
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
object Encoder11 extends AbstractVersionedEncoder with Constants with Log {

   val encoder10 = Encoder10

   override def writeHeader(r: Response, buf: ChannelBuffer, addressCache: Cache[Address, ServerAddress]) {
      val topologyResp = getTopologyResponse(r, addressCache)
      buf.writeByte(MAGIC_RES.byteValue)
      writeUnsignedLong(r.messageId, buf)
      buf.writeByte(r.operation.id.byteValue)
      buf.writeByte(r.status.id.byteValue)
      if (topologyResp != null) {
         topologyResp match {
            case t: TopologyAwareResponse => {
               if (r.clientIntel == 2)
                  encoder10.writeTopologyHeader(t, buf)
               else
                  writeHashTopologyHeader(t, buf)
            }
            case h: HashDistAware11Response => writeHashTopologyHeader(h, buf)
         }
      } else {
         buf.writeByte(0) // No topology change
      }

   }

   override def writeResponse(r: Response, buf: ChannelBuffer, cacheManager: EmbeddedCacheManager) =
      encoder10.writeResponse(r, buf, cacheManager)

   def getTopologyResponse(r: Response, addressCache: Cache[Address, ServerAddress]): AbstractTopologyResponse = {
      // If clustered, set up a cache for topology information
      if (addressCache != null) {
         r.clientIntel match {
            case 2 | 3 => {
               val viewId = addressCache.getAdvancedCache.getRpcManager.getTransport.getViewId
               if (r.topologyId != viewId) {
                  val cache = getCacheInstance(r.cacheName, addressCache.getCacheManager)
                  val config = cache.getConfiguration
                  if (r.clientIntel == 2 || !config.getCacheMode.isDistributed) {
                     TopologyAwareResponse(viewId, addressCache.values())
                  } else { // Must be 3 and distributed
                     // TODO: Retrieve hash function when we have specified functions
                     HashDistAware11Response(viewId, addressCache.values(),
                           config.getNumOwners, DEFAULT_HASH_FUNCTION_VERSION,
                           Integer.MAX_VALUE, config.getNumVirtualNodes)
                  }
               } else null
            }
            case 1 => null
         }
      } else null
   }

   private def writeHashTopologyHeader(t: TopologyAwareResponse, buf: ChannelBuffer) {
      trace("Return limited hash distribution aware header in spite of having a hash aware client %s", t)
      writeTopologyHeader(buf, t.viewId, 0, 0, 0, t.members, 0)
   }

   private def writeHashTopologyHeader(h: HashDistAware11Response, buf: ChannelBuffer) {
      trace("Write hash distribution change response header %s", h)
      writeTopologyHeader(buf, h.viewId, h.numOwners, h.hashFunction,
                          h.hashSpace, h.members, h.numVNodes)
   }

   private def writeTopologyHeader(buf: ChannelBuffer, viewId: Int,
            numOwners: Int, hashFct: Byte, hashSpace: Int,
            members: Iterable[ServerAddress], numVNodes: Int) {
      buf.writeByte(1) // Topology changed
      writeUnsignedInt(viewId, buf) // View id
      writeUnsignedShort(numOwners, buf) // Num key owners
      buf.writeByte(hashFct) // Hash function
      writeUnsignedInt(hashSpace, buf) // Hash space
      writeUnsignedInt(members.size, buf) // Num servers in topology
      writeUnsignedInt(numVNodes, buf) // Num virtual nodes
      members.foreach {address =>
         writeString(address.host, buf)
         writeUnsignedShort(address.port, buf)
         // New in 1.1, no hash id is written
      }
   }

}