/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
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

import logging.{JavaLog, Log}
import org.jboss.netty.buffer.ChannelBuffer
import org.infinispan.Cache
import org.infinispan.remoting.transport.Address
import org.infinispan.server.core.transport.ExtendedChannelBuffer._
import collection.JavaConversions._
import org.infinispan.configuration.cache.Configuration
import org.infinispan.distribution.ch.ConsistentHash
import collection.mutable.ArrayBuffer
import collection.immutable.{TreeMap, SortedMap}
import collection.mutable
import org.infinispan.util.logging.LogFactory

/**
 * Version specific encoders are included here.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
object Encoders {

   /**
    * Encoder for version 1.0 of the Hot Rod protocol.
    */
   object Encoder10 extends AbstractEncoder1x

   /**
    * Encoder for version 1.1 of the Hot Rod protocol.
    */
   object Encoder11 extends AbstractEncoder1x with Log {

      override protected def createHashDistAwareResp(lastViewId: Int,
               cfg: Configuration): AbstractHashDistAwareResponse = {
         HashDistAware11Response(lastViewId, cfg.clustering().hash().numOwners(),
               DEFAULT_HASH_FUNCTION_VERSION, Integer.MAX_VALUE,
               cfg.clustering().hash().numVirtualNodes())
      }

      override protected def writeHashTopologyHeader(
               topoResp: AbstractTopologyResponse, buf: ChannelBuffer, r: Response,
               members: Cache[Address, ServerAddress], server: HotRodServer) {
         topoResp match {
            case h: HashDistAware11Response => {
               trace("Write hash distribution change response header %s", h)
               if (h.hashFunction == 0) {
                  // When the cache is replicated, we just send the addresses without any hash ids
                  writeCommonHashTopologyHeader(buf, h.viewId, h.numOwners,
                     h.hashFunction, h.hashSpace, members.size)
                  writeUnsignedInt(1, buf) // Num virtual nodes

                  mapAsScalaMap(members).foreach { case (addr, serverAddr) =>
                     writeString(serverAddr.host, buf)
                     writeUnsignedShort(serverAddr.port, buf)
                     // Send the address' hash code as is
                     // With virtual nodes off, clients will have to normalize it
                     // With virtual nodes on, it's used as root to calculate
                     // hash code and then normalize it
                     buf.writeInt(0)
                  }
                  return
               }

               val cache = server.getCacheInstance(r.cacheName, members.getCacheManager, false)

               // This is not quite correct, as the ownership of segments on the 1.0/1.1 clients is not exactly
               // the same as on the server. But the difference appears only for (numSegment*numOwners/MAX_INT)
               // of the keys (at the "segment borders"), so it's still much better than having no hash information.
               // The idea here is to be able to be compatible with clients running version 1.0 of the protocol.
               // With time, users should migrate to version 1.2 capable clients.
               val distManager = cache.getAdvancedCache.getDistributionManager
               val ch = distManager.getConsistentHash

               val numSegments = ch.getNumSegments
               val totalNumServers = (0 until numSegments).map(i => ch.locateOwnersForSegment(i).size).sum
               writeCommonHashTopologyHeader(buf, h.viewId, h.numOwners,
                  h.hashFunction, h.hashSpace, totalNumServers)
               writeUnsignedInt(1, buf) // Num virtual nodes

               val allDenormalizedHashIds = denormalizeSegmentHashIds(ch)
               for (segmentIdx <- 0 until numSegments) {
                  val denormalizedSegmentHashIds = allDenormalizedHashIds(segmentIdx)
                  val segmentOwners = ch.locateOwnersForSegment(segmentIdx)
                  for (ownerIdx <- 0 until segmentOwners.length) {
                     val address = segmentOwners(ownerIdx % segmentOwners.size)
                     val serverAddress = members(address)
                     val hashId = denormalizedSegmentHashIds(ownerIdx)
                     log.tracef("Writing hash id %d for %s:%s", hashId, serverAddress.host, serverAddress.port)
                     writeString(serverAddress.host, buf)
                     writeUnsignedShort(serverAddress.port, buf)
                     buf.writeInt(hashId)
                  }
               }
            }
            case t: TopologyAwareResponse => {
               trace("Return limited hash distribution aware header in spite of having a hash aware client %s", t)
               val serverAddresses = members.values()
               writeCommonHashTopologyHeader(buf, t.viewId, 0, 0, 0, serverAddresses.size)
               writeUnsignedInt(0, buf) // Num virtual nodes
               serverAddresses.foreach { address =>
                  writeString(address.host, buf)
                  writeUnsignedShort(address.port, buf)
                  buf.writeInt(0) // Address' hash id
               }
            }
            case _ => throw new IllegalStateException(
               "Expected version 1.1 specific response: " + topoResp)
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
}
