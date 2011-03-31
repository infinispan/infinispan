package org.infinispan.server.hotrod

import org.infinispan.server.core.transport.{ChannelBuffer, ChannelHandlerContext, Channel, Encoder}
import OperationStatus._
import org.infinispan.server.core.transport.ChannelBuffers._
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.Cache
import org.infinispan.server.core.{CacheValue, Logging}
import org.infinispan.util.ByteArrayKey
import scala.collection.JavaConversions._

/**
 * Hot Rod specific encoder.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class HotRodEncoder(cacheManager: EmbeddedCacheManager) extends Encoder {
   import HotRodEncoder._
   import HotRodServer._

   private lazy val isClustered: Boolean = cacheManager.getGlobalConfiguration.getTransportClass != null
   private lazy val topologyCache: Cache[String, TopologyView] =
      if (isClustered) cacheManager.getCache(TopologyCacheName) else null

   override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef): AnyRef = {
      val isTrace = isTraceEnabled

      if (isTrace) trace("Encode msg %s", msg)
      val buffer: ChannelBuffer = msg match { 
         case r: Response => writeHeader(r, isTrace, getTopologyResponse(r))
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
         case g: BulkGetResponse => {
            if (isTrace) trace("About to respond to bulk get request")
            if (g.status == Success) {
               val cache: Cache[ByteArrayKey, CacheValue] = getCacheInstance(g.cacheName, cacheManager)
               var iterator = asIterator(cache.entrySet.iterator)
               if (g.count != 0) {
                  if (isTrace) trace("About to write (max) %d messages to the client", g.count)
                  iterator = iterator.take(g.count)
               }
               for (entry <- iterator) {
                  buffer.writeByte(1) // Not done
                  buffer.writeRangedBytes(entry.getKey.getData)
                  buffer.writeRangedBytes(entry.getValue.data)
               }
               buffer.writeByte(0) // Done
            }
         }
         case g: GetResponse => if (g.status == Success) buffer.writeRangedBytes(g.data.get)
         case e: ErrorResponse => buffer.writeString(e.msg)
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
                     val hashSpace = cache.getAdvancedCache.getDistributionManager.getConsistentHash.getHashSpace
                     HashDistAwareResponse(TopologyView(currentTopologyView.topologyId, currentTopologyView.members),
                           config.getNumOwners, DEFAULT_HASH_FUNCTION_VERSION, hashSpace)
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
      buffer.writeUnsignedLong(r.messageId)
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
      buffer.writeUnsignedInt(t.view.topologyId)
      buffer.writeUnsignedInt(t.view.members.size)
      t.view.members.foreach{address =>
         buffer.writeString(address.host)
         buffer.writeUnsignedShort(address.port)
      }
   }

   private def writeHashTopologyHeader(t: TopologyAwareResponse, buffer: ChannelBuffer, isTrace: Boolean) {
      if (isTrace) trace("Return limited hash distribution aware header in spite of having a hash aware client %s", t)
      buffer.writeByte(1) // Topology changed
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

   private def writeHashTopologyHeader(h: HashDistAwareResponse, buffer: ChannelBuffer, r: Response, isTrace: Boolean) {
      if (isTrace) trace("Write hash distribution change response header %s", h)
      buffer.writeByte(1) // Topology changed
      buffer.writeUnsignedInt(h.view.topologyId)
      buffer.writeUnsignedShort(h.numOwners) // Num key owners
      buffer.writeByte(h.hashFunction) // Hash function
      buffer.writeUnsignedInt(h.hashSpace) // Hash space
      buffer.writeUnsignedInt(h.view.members.size)
      h.view.members.foreach{address =>
         buffer.writeString(address.host)
         buffer.writeUnsignedShort(address.port)
         val hashId = address.hashIds.get(r.cacheName).get
         buffer.writeInt(hashId) // Address' hash id
      }
   }

}

object HotRodEncoder extends Logging {
   private val Magic = 0xA1
}
