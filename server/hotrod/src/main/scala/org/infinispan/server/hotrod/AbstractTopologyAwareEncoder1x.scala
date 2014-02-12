package org.infinispan.server.hotrod

import logging.Log
import org.infinispan.Cache
import org.infinispan.remoting.transport.Address
import org.infinispan.server.core.transport.ExtendedByteBuf._
import collection.JavaConversions._
import org.infinispan.configuration.cache.Configuration
import collection.mutable.ArrayBuffer
import org.infinispan.distribution.ch.ConsistentHash
import io.netty.buffer.ByteBuf

/**
 * Hot Rod encoder for protocol version 1.1
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
abstract class AbstractTopologyAwareEncoder1x extends AbstractEncoder1x with Constants with Log {

   override protected def createHashDistAwareResp(lastViewId: Int,
                                                  serverEndpointsMap: Map[Address, ServerAddress],
                                                  cfg: Configuration): AbstractHashDistAwareResponse = {
      HashDistAware11Response(lastViewId, serverEndpointsMap, cfg.clustering().hash().numOwners(),
            DEFAULT_HASH_FUNCTION_VERSION, Integer.MAX_VALUE,
            cfg.clustering().hash().numVirtualNodes())
   }


   override def writeHashTopologyUpdate(h: AbstractHashDistAwareResponse, server: HotRodServer, r: Response,
                                        buf: ByteBuf) {
      h match {
         case h: HashDistAware11Response => {
            writeHashTopologyUpdate11(h, server, r, buf)
         }
         case _ => throw new IllegalStateException(
            "Expected version 1.1 specific response: " + h)
      }
   }

   def writeHashTopologyUpdate11(h: HashDistAware11Response,
                               server: HotRodServer, r: Response, buf: ByteBuf) {
      trace("Write hash distribution change response header %s", h)
      if (h.hashFunction == 0) {
         writeLimitedHashTopologyUpdate(h, buf)
         return
      }

      val cache = server.getCacheInstance(r.cacheName, server.getCacheManager, false)

      // This is not quite correct, as the ownership of segments on the 1.0/1.1 clients is not exactly
      // the same as on the server. But the difference appears only for (numSegment*numOwners/MAX_INT)
      // of the keys (at the "segment borders"), so it's still much better than having no hash information.
      // The idea here is to be able to be compatible with clients running version 1.0 of the protocol.
      // With time, users should migrate to version 1.2 capable clients.
      val distManager = cache.getAdvancedCache.getDistributionManager
      val ch = distManager.getReadConsistentHash
      val numSegments = ch.getNumSegments

      // Collect all the hash ids in a collection so we can write the correct size.
      // There will be more than one hash id for each server, so we can't use a map.
      var hashIds = collection.mutable.ArrayBuffer[(ServerAddress, Int)]()
      val allDenormalizedHashIds = denormalizeSegmentHashIds(ch)
      for (segmentIdx <- 0 until numSegments) {
         val denormalizedSegmentHashIds = allDenormalizedHashIds(segmentIdx)
         val segmentOwners = ch.locateOwnersForSegment(segmentIdx)
         for (ownerIdx <- 0 until segmentOwners.length) {
            val address = segmentOwners(ownerIdx % segmentOwners.size)
            h.serverEndpointsMap.get(address) match {
               case Some(serverAddress) => {
                  val hashId = denormalizedSegmentHashIds(ownerIdx)
                  hashIds += ((serverAddress, hashId))
               }
               case None => {
                  log.tracef("Could not find member %s in the address cache", address)
               }
            }
         }
      }

      writeCommonHashTopologyHeader(buf, h.topologyId, h.numOwners,
         h.hashFunction, h.hashSpace, hashIds.size)
      writeUnsignedInt(1, buf) // Num virtual nodes

      for ((serverAddress, hashId) <- hashIds) {
         log.tracef("Writing hash id %d for %s:%s", hashId, serverAddress.host, serverAddress.port)
         writeString(serverAddress.host, buf)
         writeUnsignedShort(serverAddress.port, buf)
         buf.writeInt(hashId)
      }
   }


   override def writeLimitedHashTopologyUpdate(t: AbstractTopologyResponse, buf: ByteBuf) {
      trace("Return limited hash distribution aware header in spite of having a hash aware client %s", t)
      writeCommonHashTopologyHeader(buf, t.topologyId, 0, 0, 0, t.serverEndpointsMap.size)
      writeUnsignedInt(1, buf) // Num virtual nodes
      for (address <- t.serverEndpointsMap.values) {
         writeString(address.host, buf)
         writeUnsignedShort(address.port, buf)
         buf.writeInt(0) // Address' hash id
      }
   }

   // "Denormalize" the segments - for each hash segment, find numOwners integer values that map on the hash wheel
   // to the interval [segmentIdx*segmentSize, segmentIdx*segmentSize+leeway], leeway being hardcoded
   // on the first line of the function
   // TODO This relies on implementation details (segment layout) of DefaultConsistentHash, and won't work with any other CH
   def denormalizeSegmentHashIds(ch: ConsistentHash): Array[Seq[Int]] = {
      // This is the fraction of keys we allow to have "wrong" owners. The algorithm below takes longer
      // as this value decreases, and at some point it starts hanging (checked with an assert below)
      val leewayFraction = 0.0002
      val numOwners = ch.getNumOwners
      val numSegments = ch.getNumSegments

      val segmentSize = math.ceil(Integer.MAX_VALUE.toDouble / numSegments).toInt
      val leeway = (leewayFraction * segmentSize).toInt
      assert(leeway > 2 * numOwners, "numOwners is too big")
      val ownerHashes = new Array[collection.mutable.Map[Int, Int]](numSegments)
      for (i <- 0 until numSegments) {
         ownerHashes(i) = collection.mutable.Map[Int, Int]()
      }
      var segmentsLeft : Int = numSegments

      var i = 0
      while (segmentsLeft != 0) {
         val normalizedHash = ch.getHashFunction.hash(i) & Integer.MAX_VALUE
         if (normalizedHash % segmentSize < leeway) {
            val nextSegmentIdx = normalizedHash / segmentSize
            val segmentIdx = (nextSegmentIdx - 1 + numSegments) % numSegments
            val segmentHashes = ownerHashes(segmentIdx)
            if (segmentHashes.size < numOwners) {
               segmentHashes += (normalizedHash -> i)
               if (segmentHashes.size == numOwners) {
                  segmentsLeft -= 1
               }
            }
         }
         // Allows overflow, if we didn't find all segments in the 0..MAX_VALUE range
         i += 1
      }
      log.tracef("Found denormalized hashes: %s", ownerHashes)

      // Sort each list of hashes by the normalized hash and then return a list with only the denormalized hash
      val denormalizedHashes = ownerHashes.map(segmentHashes => segmentHashes.toSeq.sortBy(_._1).map(_._2))
      return denormalizedHashes
   }
}
