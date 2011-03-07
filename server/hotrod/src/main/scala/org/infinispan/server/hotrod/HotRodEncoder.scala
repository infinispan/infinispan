package org.infinispan.server.hotrod

import org.infinispan.server.core.transport.{ChannelBuffer, ChannelHandlerContext, Channel, Encoder}
import OperationStatus._
import org.infinispan.server.core.transport.ChannelBuffers._
import collection.mutable.ListBuffer
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.Cache
import org.infinispan.server.core.{CacheValue, Logging}
import org.infinispan.util.ByteArrayKey
import scala.collection.JavaConversions._
import collection.mutable
import collection.immutable
import org.infinispan.remoting.transport.Address

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

      if (isTrace) trace("Encode msg {0}", msg)
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
                  if (isTrace) trace("About to write (max) {0} messages to the client", g.count)
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
                           config.getNumOwners, 1, hashSpace)
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
      if (isTrace) trace("Write topology change response header {0}", t)
      buffer.writeByte(1) // Topology changed
      buffer.writeUnsignedInt(t.view.topologyId)
      buffer.writeUnsignedInt(t.view.members.size)
      t.view.members.foreach{address =>
         buffer.writeString(address.host)
         buffer.writeUnsignedShort(address.port)
      }
   }

   private def writeHashTopologyHeader(t: TopologyAwareResponse, buffer: ChannelBuffer, isTrace: Boolean) {
      if (isTrace) trace("Return limited hash distribution aware header in spite of having a hash aware client {0}", t)
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
      if (isTrace) trace("Write hash distribution change response header {0}", h)
      try {
         val computedHashIds = checkForRehashing(r, h)
         buffer.writeByte(1) // Topology changed
         buffer.writeUnsignedInt(h.view.topologyId)
         buffer.writeUnsignedShort(h.numOwners) // Num key owners
         buffer.writeByte(h.hashFunction) // Hash function
         buffer.writeUnsignedInt(h.hashSpace) // Hash space
         buffer.writeUnsignedInt(h.view.members.size)
         var hashIdUpdateRequired = false
         val updateMembers = new ListBuffer[TopologyAddress]
         h.view.members.foreach{address =>
            buffer.writeString(address.host)
            buffer.writeUnsignedShort(address.port)
            val cachedHashId = address.hashIds.get(r.cacheName)
            val hashId = computedHashIds(address.clusterAddress)
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
      } catch {
         case u: UnsupportedOperationException => {
            if (isDebugEnabled) debug("Unable to get all hash ids due to rehashing being in process. Mark as topology not changed.")
            // Rehashing is ongoing, so mark as if topology not changed
            // In next request, the client should still send the old view id
            // which the server should spot and attempt again to get all hash ids
            buffer.writeByte(0)
         }
      }

   }

   private def checkForRehashing(r: Response, h: HashDistAwareResponse): Map[Address, Int] = {
      // If we reached here, we know for sure that this is a cache configured with distribution
      val consistentHash = getCacheInstance(r.cacheName, cacheManager).getAdvancedCache.getDistributionManager.getConsistentHash
      val hashIds = mutable.Map.empty[Address, Int]
      h.view.members.foreach{address =>
         val hashId = consistentHash.getHashId(address.clusterAddress)
         hashIds += (address.clusterAddress -> hashId)
      }
      // If we reached here, no unsupported exception was thrown,
      // so no rehashing going on.
      immutable.Map[Address, Int]() ++ hashIds
   }

}

object HotRodEncoder extends Logging {
   private val Magic = 0xA1
}
