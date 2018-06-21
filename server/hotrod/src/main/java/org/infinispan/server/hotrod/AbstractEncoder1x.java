package org.infinispan.server.hotrod;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.HashFunctionPartitioner;
import org.infinispan.distribution.group.impl.GroupingPartitioner;
import org.infinispan.distribution.group.impl.PartitionerConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.Events.Event;
import org.infinispan.server.hotrod.counter.listener.ClientCounterEvent;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.transport.ExtendedByteBuf;
import org.infinispan.util.KeyValuePair;

import io.netty.buffer.ByteBuf;

/**
 * Hot Rod encoder for protocol version 1.1
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public abstract class AbstractEncoder1x implements VersionedEncoder {

   protected static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   protected final boolean trace = log.isTraceEnabled();

   @Override
   public void writeEvent(Event e, ByteBuf buf) {
      // Not implemented in this version of the protocol
   }

   @Override
   public void writeCounterEvent(ClientCounterEvent event, ByteBuf buffer) {
      // Not implemented in this version of the protocol
   }

   @Override
   public void writeHeader(Response r, ByteBuf buf, Cache<Address, ServerAddress> addressCache, HotRodServer server) {
      AbstractTopologyResponse topologyResp = getTopologyResponse(r, addressCache, server);
      buf.writeByte(Constants.MAGIC_RES);
      ExtendedByteBuf.writeUnsignedLong(r.messageId, buf);
      buf.writeByte(r.operation.getResponseOpCode());
      buf.writeByte(r.status.getCode());
      if (topologyResp != null) {
         if (topologyResp instanceof TopologyAwareResponse) {
            TopologyAwareResponse tar = (TopologyAwareResponse) topologyResp;
            if (r.clientIntel == Constants.INTELLIGENCE_TOPOLOGY_AWARE)
               writeTopologyUpdate(tar, buf);
            else
               writeLimitedHashTopologyUpdate(tar, buf);
         } else if (topologyResp instanceof AbstractHashDistAwareResponse) {
            writeHashTopologyUpdate((AbstractHashDistAwareResponse) topologyResp, server, r, buf);
         } else {
            throw new IllegalArgumentException("Unsupported response instance: " + topologyResp);
         }
      } else {
         writeNoTopologyUpdate(buf);
      }
   }

   @Override
   public void writeResponse(Response r, ByteBuf buf, EmbeddedCacheManager cacheManager, HotRodServer server) {
      if (r instanceof ResponseWithPrevious) {
         Optional<byte[]> prev = ((ResponseWithPrevious) r).previous;
         if (prev.isPresent())
            ExtendedByteBuf.writeRangedBytes(prev.get(), buf);
         else
            ExtendedByteBuf.writeUnsignedInt(0, buf);
      } else if (r instanceof StatsResponse) {
         Map<String, String> stats = ((StatsResponse) r).stats;
         ExtendedByteBuf.writeUnsignedInt(stats.size(), buf);
         for (Map.Entry<String, String> entry : stats.entrySet()) {
            ExtendedByteBuf.writeString(entry.getKey(), buf);
            ExtendedByteBuf.writeString(entry.getValue(), buf);
         }
      } else if (r instanceof GetWithVersionResponse) {
         GetWithVersionResponse gwvr = (GetWithVersionResponse) r;
         if (gwvr.status == OperationStatus.Success) {
            buf.writeLong(gwvr.dataVersion);
            ExtendedByteBuf.writeRangedBytes(gwvr.data, buf);
         }
      } else if (r instanceof GetWithMetadataResponse) {
         GetWithMetadataResponse gwmr = (GetWithMetadataResponse) r;
         if (gwmr.status == OperationStatus.Success) {
            byte flags = (gwmr.lifespan < 0 ? Constants.INFINITE_LIFESPAN : (byte) 0);
            flags |= (gwmr.maxIdle < 0 ? Constants.INFINITE_MAXIDLE : (byte) 0);
            buf.writeByte(flags);
            if (gwmr.lifespan >= 0) {
               buf.writeLong(gwmr.created);
               ExtendedByteBuf.writeUnsignedInt(gwmr.lifespan, buf);
            }
            if (gwmr.maxIdle >= 0) {
               buf.writeLong(gwmr.lastUsed);
               ExtendedByteBuf.writeUnsignedInt(gwmr.maxIdle, buf);
            }
            buf.writeLong(gwmr.dataVersion);
            ExtendedByteBuf.writeRangedBytes(gwmr.data, buf);
         }
      } else if (r instanceof BulkGetResponse) {
         if (trace)
            log.trace("About to respond to bulk get request");
         BulkGetResponse bgr = (BulkGetResponse) r;
         if (bgr.status == OperationStatus.Success) {
            int count;
            if (bgr.count != 0) {
               log.tracef("About to write (max) %d messages to the client", bgr.count);
               count = bgr.count;
            } else {
               count = Integer.MAX_VALUE;
            }
            Iterator<Map.Entry<byte[], byte[]>> iterator = bgr.entries.iterator();
            while (iterator.hasNext() && count-- > 0) {
               Map.Entry<byte[], byte[]> entry = iterator.next();
               buf.writeByte(1); // Not done
               ExtendedByteBuf.writeRangedBytes(entry.getKey(), buf);
               ExtendedByteBuf.writeRangedBytes(entry.getValue(), buf);
            }
            buf.writeByte(0); // Done
         }
      } else if (r instanceof BulkGetKeysResponse) {
         log.trace("About to respond to bulk get keys request");
         BulkGetKeysResponse bgkr = (BulkGetKeysResponse) r;
         if (bgkr.status == OperationStatus.Success) {
            while (bgkr.iterator.hasNext()) {
               byte[] key = bgkr.iterator.next();
               buf.writeByte(1); // Not done
               ExtendedByteBuf.writeRangedBytes(key, buf);
            }
            buf.writeByte(0); // Done
         }
      } else if (r instanceof GetResponse) {
         if (r.status == OperationStatus.Success) ExtendedByteBuf.writeRangedBytes(((GetResponse) r).data, buf);
      } else if (r instanceof QueryResponse) {
         ExtendedByteBuf.writeRangedBytes(((QueryResponse) r).result, buf);
      } else if (r instanceof ErrorResponse) {
         ExtendedByteBuf.writeString(((ErrorResponse) r).msg, buf);
      } else if (buf == null) {
         throw new IllegalArgumentException("Response received is unknown: " + r);
      }
   }

   AbstractTopologyResponse getTopologyResponse(Response r, Cache<Address, ServerAddress> addressCache, HotRodServer server) {
      // If clustered, set up a cache for topology information
      if (addressCache != null) {
         switch (r.clientIntel) {
            case Constants.INTELLIGENCE_TOPOLOGY_AWARE:
            case Constants.INTELLIGENCE_HASH_DISTRIBUTION_AWARE:
               // Use the request cache's topology id as the HotRod topologyId.
               AdvancedCache cache = server.getCacheInstance(null, r.cacheName, addressCache.getCacheManager(), false, true);
               RpcManager rpcManager = cache.getRpcManager();
               // Only send a topology update if the cache is clustered
               int currentTopologyId = rpcManager == null ? Constants.DEFAULT_TOPOLOGY_ID : rpcManager.getTopologyId();
               // AND if the client's topology id is smaller than the server's topology id
               if (currentTopologyId >= Constants.DEFAULT_TOPOLOGY_ID && r.topologyId < currentTopologyId)
                  return generateTopologyResponse(r, addressCache, server, currentTopologyId);
         }
      }
      return null;
   }

   private AbstractTopologyResponse generateTopologyResponse(Response r, Cache<Address, ServerAddress> addressCache,
                                                             HotRodServer server, int currentTopologyId) {
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
      AdvancedCache<byte[], byte[]> cache = server.getCacheInstance(null, r.cacheName, addressCache.getCacheManager(), false, true);
      List<Address> cacheMembers = cache.getRpcManager().getMembers();

      int responseTopologyId = currentTopologyId;
      if (!addressCache.keySet().containsAll(cacheMembers)) {
         // At least one cache member is missing from the topology cache
         int clientTopologyId = r.topologyId;
         if (currentTopologyId - clientTopologyId < 2) {
            // Postpone topology update
            return null;
         } else {
            // Send partial topology update
            responseTopologyId -= 1;
         }
      }

      Configuration config = cache.getCacheConfiguration();
      if (r.clientIntel == Constants.INTELLIGENCE_TOPOLOGY_AWARE || !config.clustering().cacheMode().isDistributed()) {
         return new TopologyAwareResponse(responseTopologyId, addressCache, 0);
      } else {
         // Must be 3 and distributed
         return createHashDistAwareResp(responseTopologyId, addressCache, config);
      }
   }

   protected AbstractHashDistAwareResponse createHashDistAwareResp(int topologyId,
                                                                   Map<Address, ServerAddress> serverEndpointsMap, Configuration cfg) {
      return new HashDistAwareResponse(topologyId, serverEndpointsMap, 0, cfg.clustering().hash().numOwners(),
            Constants.DEFAULT_CONSISTENT_HASH_VERSION_1x, Integer.MAX_VALUE);
   }

   void writeHashTopologyUpdate(AbstractHashDistAwareResponse h, HotRodServer server, Response r, ByteBuf buffer) {
      AdvancedCache<byte[], byte[]> cache = server.getCacheInstance(null, r.cacheName, server.getCacheManager(), false, true);
      DistributionManager distManager = cache.getDistributionManager();
      ConsistentHash ch = distManager.getWriteConsistentHash();

      Map<Address, ServerAddress> topologyMap = h.serverEndpointsMap;
      if (topologyMap.isEmpty()) {
         log.noMembersInHashTopology(ch, topologyMap.toString());
         buffer.writeByte(0); // Topology not changed
      } else {
         log.tracef("Write hash distribution change response header %s", h);
         // This is not quite correct, as the ownership of segments on the 1.0/1.1/1.2 clients is not exactly
         // the same as on the server. But the difference appears only for (numSegment*numOwners/MAX_INT)
         // of the keys (at the "segment borders"), so it's still much better than having no hash information.
         // The idea here is to be able to support clients running version 1.0 of the protocol.
         // TODO Need a check somewhere on startup, this only works with the default key partitioner
         int numSegments = ch.getNumSegments();
         KeyPartitioner keyPartitioner = ((PartitionerConsistentHash) ch).getKeyPartitioner();
         List<Integer> segmentHashIds = extractSegmentEndHashes(keyPartitioner);
         List<KeyValuePair<ServerAddress, Integer>> serverHashes = new ArrayList<>(numSegments);
         for (Map.Entry<Address, ServerAddress> entry : topologyMap.entrySet()) {
            for (int segmentIdx = 0; segmentIdx < numSegments; ++segmentIdx) {
               int ownerIdx = ch.locateOwnersForSegment(segmentIdx).indexOf(entry.getKey());
               if (ownerIdx >= 0) {
                  Integer segmentHashId = segmentHashIds.get(segmentIdx);
                  int hashId = (segmentHashId + ownerIdx) & Integer.MAX_VALUE;
                  serverHashes.add(new KeyValuePair<>(entry.getValue(), hashId));
               }
            }
         }

         // TODO: this seems to be numOwners * numSegments looking at above logic, this doesn't seem correct.  Seems
         // totalNumServers below should be the # of unique addresses that own at least one segment.
         int totalNumServers = serverHashes.size();
         writeCommonHashTopologyHeader(buffer, h.topologyId, h.numOwners, h.hashFunction,
               h.hashSpace, totalNumServers);
         for (KeyValuePair<ServerAddress, Integer> serverHash : serverHashes) {
            ExtendedByteBuf.writeString(serverHash.getKey().getHost(), buffer);
            ExtendedByteBuf.writeUnsignedShort(serverHash.getKey().getPort(), buffer);
            int hashId = serverHash.getValue();
            if (trace)
               // TODO: why need cast to Object....
               log.tracef("Writing hash id %d for %s:%s", (Object) hashId, serverHash.getKey().getHost(), serverHash.getKey().getPort());
            buffer.writeInt(hashId);
         }
      }
   }

   List<Integer> extractSegmentEndHashes(KeyPartitioner keyPartitioner) {
      if (keyPartitioner instanceof HashFunctionPartitioner) {
         return ((HashFunctionPartitioner) keyPartitioner).getSegmentEndHashes();
      } else if (keyPartitioner instanceof GroupingPartitioner) {
         return extractSegmentEndHashes(((GroupingPartitioner) keyPartitioner).unwrap());
      } else {
         return Collections.emptyList();
      }
   }

   void writeLimitedHashTopologyUpdate(AbstractTopologyResponse t, ByteBuf buffer) {
      log.tracef("Return limited hash distribution aware header because the client %s doesn't ", t);
      Map<Address, ServerAddress> topologyMap = t.serverEndpointsMap;
      if (topologyMap.isEmpty()) {
         log.noMembersInTopology();
         buffer.writeByte(0); // Topology not changed
      } else {
         writeCommonHashTopologyHeader(buffer, t.topologyId, 0, (byte) 0, 0, topologyMap.size());
         for (ServerAddress address : topologyMap.values()) {
            ExtendedByteBuf.writeString(address.getHost(), buffer);
            ExtendedByteBuf.writeUnsignedShort(address.getPort(), buffer);
            buffer.writeInt(0); // Address' hash id
         }
      }
   }

   void writeTopologyUpdate(TopologyAwareResponse t, ByteBuf buffer) {
      Map<Address, ServerAddress> topologyMap = t.serverEndpointsMap;
      if (topologyMap.isEmpty()) {
         log.noMembersInTopology();
         buffer.writeByte(0); // Topology not changed
      } else {
         log.tracef("Write topology change response header %s", t);
         buffer.writeByte(1); // Topology changed
         ExtendedByteBuf.writeUnsignedInt(t.topologyId, buffer);
         ExtendedByteBuf.writeUnsignedInt(topologyMap.size(), buffer);
         for (ServerAddress address : topologyMap.values()) {
            ExtendedByteBuf.writeString(address.getHost(), buffer);
            ExtendedByteBuf.writeUnsignedShort(address.getPort(), buffer);
         }
      }
   }


   void writeNoTopologyUpdate(ByteBuf buffer) {
      log.trace("Write topology response header with no change");
      buffer.writeByte(0);
   }

   protected void writeCommonHashTopologyHeader(ByteBuf buffer, int viewId,
                                                int numOwners, byte hashFct, int hashSpace, int numServers) {
      buffer.writeByte(1); // Topology changed
      ExtendedByteBuf.writeUnsignedInt(viewId, buffer);
      ExtendedByteBuf.writeUnsignedShort(numOwners, buffer); // Num key owners
      buffer.writeByte(hashFct); // Hash function
      ExtendedByteBuf.writeUnsignedInt(hashSpace, buffer); // Hash space
      ExtendedByteBuf.writeUnsignedInt(numServers, buffer);
      log.tracef("Topology will contain %d addresses", numServers);
   }

}
