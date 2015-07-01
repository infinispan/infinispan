package org.infinispan.server.hotrod

import io.netty.buffer.ByteBuf
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.remoting.transport.Address
import org.infinispan.server.core.transport.ExtendedByteBuf._
import org.infinispan.server.hotrod.HotRodServer._
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod.logging.Log
import org.infinispan.server.hotrod.util.BulkUtil
import scala.collection.JavaConversions._
import scala.collection.mutable
import org.infinispan.server.hotrod.Events._
import scala.collection.mutable.ListBuffer
import org.infinispan.remoting.rpc.RpcManager

/**
 * @author Galder Zamarreño
 */
object Encoder2x extends AbstractVersionedEncoder with Constants with Log {

   override def writeEvent(e: Event, buf: ByteBuf) {
      if (isTraceEnabled)
         log.tracef("Write event %s", e)

      buf.writeByte(MAGIC_RES.byteValue)
      writeUnsignedLong(e.messageId, buf)
      buf.writeByte(e.op.id.byteValue)
      buf.writeByte(Success.id.byteValue)
      buf.writeByte(0) // no topology change
      writeRangedBytes(e.listenerId, buf)
      e match {
         case k: KeyWithVersionEvent =>
            buf.writeByte(0) // custom marker
            buf.writeByte(if (k.isRetried) 1 else 0)
            writeRangedBytes(k.key, buf)
            buf.writeLong(k.dataVersion)
         case k: KeyEvent =>
            buf.writeByte(0) // custom marker
            buf.writeByte(if (k.isRetried) 1 else 0)
            writeRangedBytes(k.key, buf)
         case c: CustomEvent =>
            buf.writeByte(1) // custom marker
            buf.writeByte(if (c.isRetried) 1 else 0)
            writeRangedBytes(c.eventData, buf)
         case c: CustomRawEvent =>
            buf.writeByte(2) // custom raw marker
            buf.writeByte(if (c.isRetried) 1 else 0)
            writeRangedBytes(c.eventData, buf)
      }
   }

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
      val topologyMap = t.serverEndpointsMap
      if (topologyMap.isEmpty) {
         logNoMembersInTopology()
         buffer.writeByte(0) // Topology not changed
      } else {
         trace("Write topology change response header %s", t)
         buffer.writeByte(1) // Topology changed
         writeUnsignedInt(t.topologyId, buffer)
         writeUnsignedInt(topologyMap.size, buffer)
         for (address <- topologyMap.values) {
            writeString(address.host, buffer)
            writeUnsignedShort(address.port, buffer)
         }
      }
   }

   private def writeEmptyHashInfo(t: AbstractTopologyResponse, buffer: ByteBuf) {
      trace("Return limited hash distribution aware header because the client %s doesn't ", t)
      buffer.writeByte(0) // Hash Function Version
      writeUnsignedInt(t.numSegments, buffer)
   }

   private def writeHashTopologyUpdate(h: HashDistAware20Response, cache: Cache, buf: ByteBuf) {
      // Calculate members first, in case there are no members
      val ch = SecurityActions.getCacheDistributionManager(cache).getReadConsistentHash
      val members = h.serverEndpointsMap.filter { case (addr, serverAddr) =>
         ch.getMembers.contains(addr)
      }

      if (isTraceEnabled) {
         trace(s"Topology cache contains: ${h.serverEndpointsMap}")
         trace(s"After read consistent hash filter, members are: $members")
      }

      if (members.isEmpty) {
         logNoMembersInHashTopology(ch, h.serverEndpointsMap.toString())
         buf.writeByte(0) // Topology not changed
      } else {
         trace("Write hash distribution change response header %s", h)
         buf.writeByte(1) // Topology changed
         writeUnsignedInt(h.topologyId, buf) // Topology ID

         // Write members
         var indexCount = -1
         writeUnsignedInt(members.size, buf)
         val indexedMembers = members.map { case (addr, serverAddr) =>
            writeString(serverAddr.host, buf)
            writeUnsignedShort(serverAddr.port, buf)
            indexCount += 1
            addr -> indexCount // easier indexing
         }

         // Write segment information
         val numSegments = ch.getNumSegments
         buf.writeByte(h.hashFunction) // Hash function
         writeUnsignedInt(numSegments, buf)

         for (segmentId <- 0 until numSegments) {
            val owners = ch.locateOwnersForSegment(segmentId).filter(members.contains)
            val ownersSize = owners.size
            if (ownersSize == 0) {
               // When sending partial updates, number of owners could be 0,
               // in which case just take the first member in the list.
               buf.writeByte(1)
               writeUnsignedInt(0, buf)
            } else {
               buf.writeByte(ownersSize)
               owners.foreach { ownerAddr =>
                  indexedMembers.get(ownerAddr) match {
                     case Some(index) => writeUnsignedInt(index, buf)
                     case None => // Do not add to indexes
                  }
               }
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
               val rpcManager = SecurityActions.getCacheRpcManager(cache)
               // Only send a topology update if the cache is clustered
               val currentTopologyId = rpcManager match {
                  case null => DEFAULT_TOPOLOGY_ID
                  case _ => rpcManager.getTopologyId
               }
               // AND if the client's topology id is smaller than the server's topology id
               if (currentTopologyId >= DEFAULT_TOPOLOGY_ID && r.topologyId < currentTopologyId)
                  generateTopologyResponse(r, addressCache, cache, rpcManager, currentTopologyId)
               else None
            }
            case INTELLIGENCE_BASIC => None
         }
      } else None
   }

   private def generateTopologyResponse(r: Response, addressCache: AddressCache,
           cache: Cache, rpcManager: RpcManager, currentTopologyId: Int): Option[AbstractTopologyResponse] = {
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
      val cacheMembers = rpcManager.getMembers
      val serverEndpoints = addressCache.toMap

      var topologyId = currentTopologyId
      val isTrace = isTraceEnabled

      if (isTrace) {
         tracef("Check for partial topologies: members=%s, endpoints=%s, client-topology=%s, server-topology=%s",
            cacheMembers, serverEndpoints, r.topologyId, topologyId)
      }

      if (!serverEndpoints.keySet.containsAll(cacheMembers)) {
         // At least one cache member is missing from the topology cache
         val clientTopologyId = r.topologyId
         if (currentTopologyId - clientTopologyId < 2) {
            if (isTrace) trace("Postpone topology update")
            return None // Postpone topology update
         } else {
            // Send partial topology update
            topologyId -= 1
            if (isTrace) trace("Send partial topology update with topology id %s", topologyId)
         }
      }
      val numSegments = Option(SecurityActions.getCacheDistributionManager(cache)).map(_.getReadConsistentHash).map(_.getNumSegments).getOrElse(0)
      val config = SecurityActions.getCacheConfiguration(cache)
      if (r.clientIntel == INTELLIGENCE_TOPOLOGY_AWARE || !config.clustering().cacheMode().isDistributed) {
         Some(TopologyAwareResponse(topologyId, serverEndpoints, numSegments))
      } else {
         Some(HashDistAware20Response(topologyId, serverEndpoints, numSegments, DEFAULT_CONSISTENT_HASH_VERSION))
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
         case g: GetAllResponse =>
           if (isTraceEnabled)
             log.trace("About to respond to getAll request")
           if (g.status == Success) {
             writeUnsignedInt(g.entries.size, buf)
             val iterator = asScalaIterator(g.entries.iterator)
             for (entry <- iterator) {
                writeRangedBytes(entry._1, buf)
                writeRangedBytes(entry._2, buf)
             }
           }
         case g: GetResponse =>
            if (g.status == Success) writeRangedBytes(g.data.get, buf)
         case q: QueryResponse =>
            writeRangedBytes(q.result, buf)
         case a: AuthMechListResponse => {
            writeUnsignedInt(a.mechs.size, buf)
            for(mech <- a.mechs) {
               writeString(mech, buf)
            }
         }
         case a: AuthResponse => {
            if (a.challenge != null) {
               buf.writeBoolean(false)
               writeRangedBytes(a.challenge, buf)
            } else {
               buf.writeBoolean(true)
               writeUnsignedInt(0, buf)
            }
         }
         case s: SizeResponse => writeUnsignedLong(s.size, buf)
         case e: ExecResponse =>
            writeRangedBytes(e.result, buf)
         case r: IterationStartResponse => writeString(r.iterationId, buf)
         case r: IterationNextResponse =>
            writeRangedBytes(r.iterationResult.segmentsToBytes, buf)
            val entries = r.iterationResult.entrySeq
            writeUnsignedInt(entries.size, buf)
            entries.foreach { e =>
               writeRangedBytes(e._1.asInstanceOf[Bytes],buf)
               writeRangedBytes(e._2.asInstanceOf[Bytes],buf)
            }
         case e: ErrorResponse => writeString(e.msg, buf)
         case _ => if (buf == null)
            throw new IllegalArgumentException("Response received is unknown: " + r)
      }
   }
}
