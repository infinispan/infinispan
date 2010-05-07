package org.infinispan.server.hotrod

import org.infinispan.server.core.Logging
import org.infinispan.server.core.transport.{ChannelBuffer, ChannelHandlerContext, Channel, Encoder}
import OperationStatus._
import org.infinispan.server.core.transport.ChannelBuffers._
import org.infinispan.manager.CacheManager
import org.infinispan.Cache
import collection.mutable.ListBuffer

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

class HotRodEncoder(cacheManager: CacheManager) extends Encoder {
   import HotRodEncoder._
   import HotRodServer._
   private lazy val topologyCache: Cache[String, TopologyView] = cacheManager.getCache(TopologyCacheName)   

   override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef): AnyRef = {
      trace("Encode msg {0}", msg)
      val buffer: ChannelBuffer = msg match { 
         case r: Response => writeHeader(r)
      }
      msg match {
         case r: ResponseWithPrevious => {
            if (r.previous == None)
               buffer.writeUnsignedInt(0)
            else
               buffer.writeRangedBytes(r.previous.get)
         }
         case s: StatsResponse => {
            buffer.writeUnsignedInt(s.stats.size)
            for ((key, value) <- s.stats) {
               buffer.writeString(key)
               buffer.writeString(value)
            }
         }
         case g: GetWithVersionResponse => {
            if (g.status == Success) {
               buffer.writeLong(g.version)
               buffer.writeRangedBytes(g.data.get)
            }
         }
         case g: GetResponse => if (g.status == Success) buffer.writeRangedBytes(g.data.get)
         case e: ErrorResponse => buffer.writeString(e.msg)
         case _ => if (buffer == null) throw new IllegalArgumentException("Response received is unknown: " + msg);         
      }
      buffer
   }

   private def writeHeader(r: Response): ChannelBuffer = {
      val buffer = dynamicBuffer
      buffer.writeByte(Magic.byteValue)
      buffer.writeUnsignedLong(r.messageId)
      buffer.writeByte(r.operation.id.byteValue)
      buffer.writeByte(r.status.id.byteValue)
      if (r.topologyResponse != None) {
         buffer.writeByte(1) // Topology changed
         r.topologyResponse.get match {
            case t: TopologyAwareResponse => {
               if (r.clientIntel == 2)
                  writeTopologyHeader(t, buffer)
               else
                  writeHashTopologyHeader(t, buffer)
            }
            case h: HashDistAwareResponse => writeHashTopologyHeader(h, buffer, r)
         }
      } else {
         buffer.writeByte(0) // No topology change
      }
      buffer
   }

   private def writeTopologyHeader(t: TopologyAwareResponse, buffer: ChannelBuffer) {
      trace("Write topology change response header {0}", t)
      buffer.writeUnsignedInt(t.view.topologyId)
      buffer.writeUnsignedInt(t.view.members.size)
      t.view.members.foreach{address =>
         buffer.writeString(address.host)
         buffer.writeUnsignedShort(address.port)
      }
   }

   private def writeHashTopologyHeader(t: TopologyAwareResponse, buffer: ChannelBuffer) {
      trace("Return limited hash distribution aware header in spite of having a hash aware client {0}", t)
      buffer.writeUnsignedInt(t.view.topologyId)
      buffer.writeUnsignedShort(0) // Num key owners
      buffer.writeByte(0) // Hash function
      buffer.writeUnsignedInt(0) // Hash space
      buffer.writeUnsignedInt(t.view.members.size)
      t.view.members.foreach{address =>
         buffer.writeString(address.host)
         buffer.writeUnsignedShort(address.port)
         buffer.writeInt(0) // Address' hash id
      }
   }

   private def writeHashTopologyHeader(h: HashDistAwareResponse, buffer: ChannelBuffer, r: Response) {
      trace("Write hash distribution change response header {0}", h)
      buffer.writeUnsignedInt(h.view.topologyId)
      buffer.writeUnsignedShort(h.numOwners) // Num key owners
      buffer.writeByte(h.hashFunction) // Hash function
      buffer.writeUnsignedInt(h.hashSpace) // Hash space
      buffer.writeUnsignedInt(h.view.members.size)
      var hashIdUpdateRequired = false
      // If we reached here, we know for sure that this is a cache configured with distribution
      val consistentHash = cacheManager.getCache(r.cacheName).getAdvancedCache.getDistributionManager.getConsistentHash
      val updateMembers = new ListBuffer[TopologyAddress]
      h.view.members.foreach{address =>
         buffer.writeString(address.host)
         buffer.writeUnsignedShort(address.port)
         val cachedHashId = address.hashIds.get(r.cacheName)
         val hashId = consistentHash.getHashId(address.clusterAddress)
         val newAddress =
            // If distinct or not present, cached hash id needs updating
            if (cachedHashId == None || cachedHashId.get != hashId) {
               if (!hashIdUpdateRequired) hashIdUpdateRequired = true
               val newHashIds = address.hashIds + (r.cacheName -> hashId)
               address.copy(hashIds = newHashIds)
            } else {
               address
            }
         updateMembers += newAddress
         buffer.writeInt(hashId) // Address' hash id
      }
      // At least a hash id had to be updated in the view. Take the view copy and distribute it around the cluster
      if (hashIdUpdateRequired) {
         val viewCopy = h.view.copy(members = updateMembers.toList)
         topologyCache.replace("view", h.view, viewCopy)
      }
   }

}

object HotRodEncoder extends Logging {
   private val Magic = 0xA1
}