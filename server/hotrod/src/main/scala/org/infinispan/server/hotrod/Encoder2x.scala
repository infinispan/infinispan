package org.infinispan.server.hotrod

import io.netty.buffer.ByteBuf
import org.infinispan.distribution.ch.DefaultConsistentHash
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.remoting.transport.Address
import org.infinispan.server.core.transport.ExtendedByteBuf._
import org.infinispan.server.hotrod.HotRodServer._
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod.logging.Log
import org.infinispan.server.hotrod.util.BulkUtil
import scala.Some
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, ArrayBuffer}

/**
 * @author Galder Zamarreño
 */
object Encoder2x extends AbstractVersionedEncoder with Constants with Log {

   override def writeHeader(r: Response, buf: ByteBuf, addressCache: AddressCache, server: HotRodServer): Unit = {
      val newTopology = getTopologyResponse(r, addressCache, server)
      buf.writeByte(MAGIC_RES.byteValue)
      writeUnsignedLong(r.messageId, buf)
      buf.writeByte(r.operation.id.byteValue)
      buf.writeByte(r.status.id.byteValue)
      newTopology match {
         case Some(topology) => topology match {
            case t: TopologyAwareResponse =>
               writeTopologyUpdate(t, buf)
               if (r.clientIntel == INTELLIGENCE_HASH_DISTRIBUTION_AWARE)
                  writeEmptyHashInfo(t, buf)
            case h: HashDistAware20Response =>
               val cache = server.getCacheInstance(r.cacheName, addressCache.getCacheManager, skipCacheCheck = false)
               writeHashTopologyUpdate(h, cache, buf)
         }
         case None =>
            trace("Write topology response header with no change")
            buf.writeByte(0)
      }
   }

   private def writeTopologyUpdate(t: TopologyAwareResponse, buffer: ByteBuf) {
      trace("Write topology change response header %s", t)
      buffer.writeByte(1) // Topology changed
      writeUnsignedInt(t.topologyId, buffer)
      writeUnsignedInt(t.serverEndpointsMap.size, buffer)
      for (address <- t.serverEndpointsMap.values) {
         writeString(address.host, buffer)
         writeUnsignedShort(address.port, buffer)
      }
   }

   private def writeEmptyHashInfo(t: AbstractTopologyResponse, buffer: ByteBuf) {
      trace("Return limited hash distribution aware header because the client %s doesn't ", t)
      buffer.writeByte(0) // Hash Function Version
      buffer.writeByte(0) // Num segments in topology
   }

   private def writeHashTopologyUpdate(h: HashDistAware20Response, cache: Cache, buf: ByteBuf) {
      trace("Write hash distribution change response header %s", h)
      buf.writeByte(1) // Topology changed
      writeUnsignedInt(h.topologyId, buf)
      writeUnsignedInt(h.serverEndpointsMap.size, buf)
      // Write known servers in topology
      val memberIndexes = mutable.Map.empty[Address, Int]
      var indexCount = 0
      for ((address, serverAddress) <- h.serverEndpointsMap) {
         writeString(serverAddress.host, buf)
         writeUnsignedShort(serverAddress.port, buf)
         memberIndexes += (address -> indexCount)
         indexCount += 1
      }

      // Write segment information
      val ch = cache.getDistributionManager.getReadConsistentHash
      val numSegments = ch.getNumSegments
      buf.writeByte(h.hashFunction) // Hash function
      writeUnsignedInt(numSegments, buf)
      for (segmentId <- 0 until numSegments) {
         val owners = ch.locateOwnersForSegment(segmentId)
         val numOwnersToSend = Math.min(2, owners.size())
         buf.writeByte(numOwnersToSend)
         owners.take(numOwnersToSend).foreach { ownerAddr =>
            memberIndexes.get(ownerAddr) match {
               case Some(index) => writeUnsignedInt(index, buf)
               case None => // Do not add to indexes
            }
         }
      }
   }

   private def getTopologyResponse(r: Response, addressCache: AddressCache, server: HotRodServer): Option[AbstractTopologyResponse] = {
      // If clustered, set up a cache for topology information
      if (addressCache != null) {
         r.clientIntel match {
            case INTELLIGENCE_TOPOLOGY_AWARE | INTELLIGENCE_HASH_DISTRIBUTION_AWARE => {
               val cache = server.getCacheInstance(r.cacheName, addressCache.getCacheManager, skipCacheCheck = false)
               // Use the request cache's topology id as the HotRod topologyId.
               val rpcManager = cache.getRpcManager
               // Only send a topology update if the cache is clustered
               val currentTopologyId = rpcManager match {
                  case null => DEFAULT_TOPOLOGY_ID
                  case _ => rpcManager.getTopologyId
               }
               // AND if the client's topology id is smaller than the server's topology id
               if (currentTopologyId >= DEFAULT_TOPOLOGY_ID && r.topologyId < currentTopologyId)
                  generateTopologyResponse(r, addressCache, cache, currentTopologyId)
               else None
            }
            case INTELLIGENCE_BASIC => None
         }
      } else None
   }

   private def generateTopologyResponse(r: Response, addressCache: AddressCache,
           cache: Cache, currentTopologyId: Int): Option[AbstractTopologyResponse] = {
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
      val cacheMembers = cache.getRpcManager.getMembers
      val serverEndpoints = addressCache.toMap
      var topologyId = currentTopologyId
      if (!serverEndpoints.keySet.containsAll(cacheMembers)) {
         // At least one cache member is missing from the topology cache
         val clientTopologyId = r.topologyId
         if (currentTopologyId - clientTopologyId < 2) {
            return None // Postpone topology update
         } else {
            // Send partial topology update
            topologyId -= 1
         }
      }

      val config = cache.getCacheConfiguration
      if (r.clientIntel == INTELLIGENCE_TOPOLOGY_AWARE || !config.clustering().cacheMode().isDistributed) {
         Some(TopologyAwareResponse(topologyId, serverEndpoints))
      } else {
         Some(HashDistAware20Response(topologyId, serverEndpoints, DEFAULT_CONSISTENT_HASH_VERSION))
      }
   }

   override def writeResponse(r: Response, buf: ByteBuf, cacheManager: EmbeddedCacheManager, server: HotRodServer): Unit = {
      r match {
         case r: ResponseWithPrevious =>
            if (r.previous == None)
               writeUnsignedInt(0, buf)
            else
               writeRangedBytes(r.previous.get, buf)
         case s: StatsResponse =>
            writeUnsignedInt(s.stats.size, buf)
            for ((key, value) <- s.stats) {
               writeString(key, buf)
               writeString(value, buf)
            }
         case g: GetWithVersionResponse =>
            if (g.status == Success) {
               buf.writeLong(g.dataVersion)
               writeRangedBytes(g.data.get, buf)
            }
         case g: GetWithMetadataResponse =>
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
         case g: BulkGetResponse =>
            log.trace("About to respond to bulk get request")
            if (g.status == Success) {
               val cache: Cache = server.getCacheInstance(g.cacheName, cacheManager, false)
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
         case g: BulkGetKeysResponse =>
            log.trace("About to respond to bulk get keys request")
            if (g.status == Success) {
               val cache: Cache = server.getCacheInstance(g.cacheName, cacheManager, false)
               val keys = BulkUtil.getAllKeys(cache, g.scope)
               val iterator = asScalaIterator(keys.iterator)
               for (key <- iterator) {
                  buf.writeByte(1) // Not done
                  writeRangedBytes(key, buf)
               }
               buf.writeByte(0) // Done
            }
         case g: GetResponse =>
            if (g.status == Success) writeRangedBytes(g.data.get, buf)
         case q: QueryResponse =>
            writeRangedBytes(q.result, buf)
         case e: ErrorResponse => writeString(e.msg, buf)
         case _ => if (buf == null)
            throw new IllegalArgumentException("Response received is unknown: " + r)
      }
   }

}
