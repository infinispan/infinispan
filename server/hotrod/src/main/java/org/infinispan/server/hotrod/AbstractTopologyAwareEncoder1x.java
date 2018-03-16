package org.infinispan.server.hotrod;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.hash.Hash;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.HashFunctionPartitioner;
import org.infinispan.distribution.group.impl.GroupingPartitioner;
import org.infinispan.distribution.group.impl.PartitionerConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.transport.ExtendedByteBuf;
import org.infinispan.util.KeyValuePair;

import io.netty.buffer.ByteBuf;

/**
 * Hot Rod encoder for protocol version 1.1
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
public abstract class AbstractTopologyAwareEncoder1x extends AbstractEncoder1x {

   @Override
   protected AbstractHashDistAwareResponse createHashDistAwareResp(int topologyId,
                                                                   Map<Address, ServerAddress> serverEndpointsMap, Configuration cfg) {
      return new HashDistAware11Response(topologyId, serverEndpointsMap, cfg.clustering().hash().numOwners(),
            Constants.DEFAULT_CONSISTENT_HASH_VERSION_1x, Integer.MAX_VALUE, 1);
   }

   @Override
   void writeHashTopologyUpdate(AbstractHashDistAwareResponse h, HotRodServer server, HotRodHeader header, ByteBuf buffer) {
      if (h instanceof HashDistAware11Response) {
         writeHashTopologyUpdate11((HashDistAware11Response) h, server, header, buffer);
      } else {
         throw new IllegalStateException(
               "Expected version 1.1 specific response: " + h);
      }
   }

   void writeHashTopologyUpdate11(HashDistAware11Response h, HotRodServer server, HotRodHeader header, ByteBuf buf) {
      log.tracef("Write hash distribution change response header %s", h);
      if (h.hashFunction == 0) {
         writeLimitedHashTopologyUpdate(h, buf);
         return;
      }

      AdvancedCache<byte[], byte[]> cache = server.getCacheInstance(null, header.cacheName, server.getCacheManager(), false, true);

      // This is not quite correct, as the ownership of segments on the 1.0/1.1 clients is not exactly
      // the same as on the server. But the difference appears only for (numSegment*numOwners/MAX_INT)
      // of the keys (at the "segment borders"), so it's still much better than having no hash information.
      // The idea here is to be able to support clients running version 1.0 of the protocol.
      // With time, users should migrate to version 1.2 capable clients.
      DistributionManager distManager = cache.getDistributionManager();
      ConsistentHash ch = distManager.getReadConsistentHash();
      int numSegments = ch.getNumSegments();

      // Collect all the hash ids in a collection so we can write the correct size.
      // There will be more than one hash id for each server, so we can't use a map.
      List<KeyValuePair<ServerAddress, Integer>> hashIds = new ArrayList<>(numSegments);
      List<Integer>[] allDenormalizedHashIds = denormalizeSegmentHashIds(ch);
      for (int segmentIdx = 0; segmentIdx < numSegments; ++segmentIdx) {
         List<Integer> denormalizedSegmentHashIds = allDenormalizedHashIds[segmentIdx];
         List<Address> segmentOwners = ch.locateOwnersForSegment(segmentIdx);
         for (int ownerIdx = 0; ownerIdx < segmentOwners.size(); ++ownerIdx) {
            Address address = segmentOwners.get(ownerIdx % segmentOwners.size());
            ServerAddress serverAddress = h.serverEndpointsMap.get(address);
            if (serverAddress != null) {
               Integer hashId = denormalizedSegmentHashIds.get(ownerIdx);
               hashIds.add(new KeyValuePair<>(serverAddress, hashId));
            } else {
               log.tracef("Could not find member %s in the address cache", address);
            }
         }
      }

      writeCommonHashTopologyHeader(buf, h.topologyId, h.numOwners,
            h.hashFunction, h.hashSpace, hashIds.size());
      ExtendedByteBuf.writeUnsignedInt(1, buf); // Num virtual nodes

      for (KeyValuePair<ServerAddress, Integer> serverHash : hashIds) {
         // TODO: why need cast to Object....
         log.tracef("Writing hash id %d for %s:%s", (Object) serverHash.getValue(), serverHash.getKey().getHost(),
               serverHash.getKey().getPort());
         ExtendedByteBuf.writeString(serverHash.getKey().getHost(), buf);
         ExtendedByteBuf.writeUnsignedShort(serverHash.getKey().getPort(), buf);
         buf.writeInt(serverHash.getValue());
      }
   }

   @Override
   void writeLimitedHashTopologyUpdate(AbstractTopologyResponse t, ByteBuf buffer) {
      log.tracef("Return limited hash distribution aware header in spite of having a hash aware client %s", t);
      writeCommonHashTopologyHeader(buffer, t.topologyId, 0, (byte) 0, 0, t.serverEndpointsMap.size());
      ExtendedByteBuf.writeUnsignedInt(1, buffer); // Num virtual nodes
      for (ServerAddress address : t.serverEndpointsMap.values()) {
         ExtendedByteBuf.writeString(address.getHost(), buffer);
         ExtendedByteBuf.writeUnsignedShort(address.getPort(), buffer);
         buffer.writeInt(0); // Address' hash id
      }
   }

   // "Denormalize" the segments - for each hash segment, find numOwners integer values that map on the hash wheel
   // to the interval [segmentIdx*segmentSize, segmentIdx*segmentSize+leeway], leeway being hardcoded
   // on the first line of the function
   // TODO This relies on implementation details (segment layout) of DefaultConsistentHash, and won't work with any other CH
   List<Integer>[] denormalizeSegmentHashIds(ConsistentHash ch) {
      // This is the fraction of keys we allow to have "wrong" owners. The algorithm below takes longer
      // as this value decreases, and at some point it starts hanging (checked with an assert below)
      double leewayFraction = 0.0002;
      int numOwners = ch.getNumOwners();
      int numSegments = ch.getNumSegments();

      int segmentSize = (int) Math.ceil((double) Integer.MAX_VALUE / numSegments);
      int leeway = (int) (leewayFraction * segmentSize);
      assert (leeway > 2 * numOwners);
      Map<Integer, Integer>[] ownerHashes = new Map[numSegments];
      for (int i = 0; i < numSegments; ++i) {
         ownerHashes[i] = new HashMap<>();
      }

      KeyPartitioner keyPartitioner = ((PartitionerConsistentHash) ch).getKeyPartitioner();
      extractHash(keyPartitioner).ifPresent(h -> {
         int i = 0;
         int segmentsLeft = numSegments;
         while (segmentsLeft != 0) {
            int normalizedHash = h.hash(i) & Integer.MAX_VALUE;
            if (normalizedHash % segmentSize < leeway) {
               int nextSegmentIdx = normalizedHash / segmentSize;
               int segmentIdx = (nextSegmentIdx - 1 + numSegments) % numSegments;
               Map<Integer, Integer> segmentHashes = ownerHashes[segmentIdx];
               if (segmentHashes.size() < numOwners) {
                  segmentHashes.put(normalizedHash, i);
                  if (segmentHashes.size() == numOwners) {
                     segmentsLeft -= 1;
                  }
               }
            }
            // Allows overflow, if we didn't find all segments in the 0..MAX_VALUE range
            i += 1;
         }
      });
      log.tracef("Found denormalized hashes: %s", ownerHashes);

      List<Integer>[] results = new List[ownerHashes.length];

      // Sort each list of hashes by the normalized hash and then return a list with only the denormalized hash
      int i = 0;
      for (Map<Integer, Integer> ownerHash : ownerHashes) {
         results[i++] = ownerHash.entrySet().stream()
               .sorted(Comparator.comparing(Map.Entry::getKey))
               .map(Map.Entry::getValue)
               .collect(Collectors.toList());
      }

      return results;
   }

   Optional<Hash> extractHash(KeyPartitioner keyPartitioner) {
      if (keyPartitioner instanceof HashFunctionPartitioner) {
         return Optional.of(((HashFunctionPartitioner) keyPartitioner).getHash());
      } else if (keyPartitioner instanceof GroupingPartitioner) {
         return extractHash(((GroupingPartitioner) keyPartitioner).unwrap());
      } else {
         return Optional.empty();
      }
   }
}
