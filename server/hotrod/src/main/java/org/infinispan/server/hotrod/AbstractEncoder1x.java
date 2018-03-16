package org.infinispan.server.hotrod;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.HashFunctionPartitioner;
import org.infinispan.distribution.group.impl.GroupingPartitioner;
import org.infinispan.distribution.group.impl.PartitionerConsistentHash;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.hotrod.Events.Event;
import org.infinispan.server.hotrod.counter.listener.ClientCounterEvent;
import org.infinispan.server.hotrod.iteration.IterableIterationResult;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.transport.ExtendedByteBuf;
import org.infinispan.stats.Stats;
import org.infinispan.util.KeyValuePair;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

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
   public ByteBuf authResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, byte[] challenge) {
      throw new UnsupportedOperationException();
   }

   @Override
   public ByteBuf authMechListResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, Set<String> mechs) {
      throw new UnsupportedOperationException();
   }

   @Override
   public ByteBuf notExecutedResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, byte[] prev) {
      return valueResponse(header, server, alloc, OperationStatus.OperationNotExecuted, prev);
   }

   @Override
   public ByteBuf notExistResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc) {
      if (header.hasFlag(ProtocolFlag.ForceReturnPreviousValue)) {
         return valueResponse(header, server, alloc, OperationStatus.KeyDoesNotExist, null);
      } else {
         return emptyResponse(header, server, alloc, OperationStatus.KeyDoesNotExist);
      }
   }

   @Override
   public ByteBuf valueResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, OperationStatus status, byte[] prev) {
      ByteBuf buf = writeHeader(header, server, alloc, status);
      if (prev == null) {
         buf.writeByte(0);
      } else {
         ExtendedByteBuf.writeRangedBytes(prev, buf);
      }
      if (trace) {
         log.tracef("Write response to %s messageId=%d status=%s prev=%s", header.op, header.messageId, status, Util.printArray(prev));
      }
      return buf;
   }

   @Override
   public ByteBuf successResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, byte[] result) {
      return valueResponse(header, server, alloc, OperationStatus.Success, result);
   }

   @Override
   public ByteBuf transactionResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, int xaReturnCode) {
      throw new UnsupportedOperationException();
   }

   @Override
   public ByteBuf errorResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, String message, OperationStatus status) {
      ByteBuf buf = writeHeader(header, server, alloc, status);
      ExtendedByteBuf.writeString(message, buf);
      return buf;
   }

   @Override
   public ByteBuf bulkGetResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, int size, CacheSet<Map.Entry<byte[], byte[]>> entries) {
      ByteBuf buf = writeHeader(header, server, alloc, OperationStatus.Success);
      int count;
      if (size != 0) {
         log.tracef("About to write (max) %d messages to the client", size);
         count = size;
      } else {
         count = Integer.MAX_VALUE;
      }
      Iterator<Map.Entry<byte[], byte[]>> iterator = entries.iterator();
      while (iterator.hasNext() && count-- > 0) {
         Map.Entry<byte[], byte[]> entry = iterator.next();
         buf.writeByte(1); // Not done
         ExtendedByteBuf.writeRangedBytes(entry.getKey(), buf);
         ExtendedByteBuf.writeRangedBytes(entry.getValue(), buf);
      }
      buf.writeByte(0); // Done
      return buf;
   }

   @Override
   public ByteBuf emptyResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, OperationStatus status) {
      return writeHeader(header, server, alloc, status);
   }

   @Override
   public ByteBuf statsResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, Stats stats, NettyTransport transport, ComponentRegistry cacheRegistry) {
      ByteBuf buf = writeHeader(header, server, alloc, OperationStatus.Success);
      ExtendedByteBuf.writeUnsignedInt(11, buf);
      writePair(buf, "timeSinceStart", String.valueOf(stats.getTimeSinceStart()));
      writePair(buf, "currentNumberOfEntries", String.valueOf(stats.getCurrentNumberOfEntries()));
      writePair(buf, "totalNumberOfEntries", String.valueOf(stats.getTotalNumberOfEntries()));
      writePair(buf, "stores", String.valueOf(stats.getStores()));
      writePair(buf, "retrievals", String.valueOf(stats.getRetrievals()));
      writePair(buf, "hits", String.valueOf(stats.getHits()));
      writePair(buf, "misses", String.valueOf(stats.getMisses()));
      writePair(buf, "removeHits", String.valueOf(stats.getRemoveHits()));
      writePair(buf, "removeMisses", String.valueOf(stats.getRemoveMisses()));
      writePair(buf, "totalBytesRead", String.valueOf(transport.getTotalBytesRead()));
      writePair(buf, "totalBytesWritten", String.valueOf(transport.getTotalBytesWritten()));
      return buf;
   }

   private void writePair(ByteBuf buf, String key, String value) {
      ExtendedByteBuf.writeString(key, buf);
      ExtendedByteBuf.writeString(value, buf);
   }

   @Override
   public ByteBuf getWithMetadataResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, CacheEntry<byte[], byte[]> entry) {
      ByteBuf buf = writeHeader(header, server, alloc, OperationStatus.Success);
      int lifespan = MetadataUtils.extractLifespan(entry);
      int maxIdle = MetadataUtils.extractMaxIdle(entry);
      byte flags = (lifespan < 0 ? Constants.INFINITE_LIFESPAN : (byte) 0);
      flags |= (maxIdle < 0 ? Constants.INFINITE_MAXIDLE : (byte) 0);
      buf.writeByte(flags);
      if (lifespan >= 0) {
         buf.writeLong(MetadataUtils.extractCreated(entry));
         ExtendedByteBuf.writeUnsignedInt(lifespan, buf);
      }
      if (maxIdle >= 0) {
         buf.writeLong(MetadataUtils.extractLastUsed(entry));
         ExtendedByteBuf.writeUnsignedInt(maxIdle, buf);
      }
      buf.writeLong(MetadataUtils.extractVersion(entry));
      ExtendedByteBuf.writeRangedBytes(entry.getValue(), buf);
      return buf;
   }

   @Override
   public ByteBuf getStreamResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, int offset, CacheEntry<byte[], byte[]> entry) {
      throw new UnsupportedOperationException();
   }

   @Override
   public ByteBuf getAllResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, Map<byte[], byte[]> map) {
      throw new UnsupportedOperationException();
   }

   @Override
   public ByteBuf bulkGetKeysResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, CloseableIterator<byte[]> iterator) {
      ByteBuf buf = writeHeader(header, server, alloc, OperationStatus.Success);
      while (iterator.hasNext()) {
         byte[] key = iterator.next();
         buf.writeByte(1); // Not done
         ExtendedByteBuf.writeRangedBytes(key, buf);
      }
      buf.writeByte(0); // Done
      return buf;
   }

   @Override
   public ByteBuf iterationStartResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, String iterationId) {
      throw new UnsupportedOperationException();
   }

   @Override
   public ByteBuf iterationNextResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, IterableIterationResult iterationResult) {
      throw new UnsupportedOperationException();
   }

   @Override
   public ByteBuf counterConfigurationResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, CounterConfiguration configuration) {
      throw new UnsupportedOperationException();
   }

   @Override
   public ByteBuf counterNamesResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, Collection<String> counterNames) {
      throw new UnsupportedOperationException();
   }

   @Override
   public ByteBuf multimapCollectionResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, OperationStatus status, Collection<byte[]> values) {
      throw new UnsupportedOperationException();
   }

   @Override
   public ByteBuf multimapEntryResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, OperationStatus status, CacheEntry<WrappedByteArray, Collection<WrappedByteArray>> ce, Collection<byte[]> result) {
      throw new UnsupportedOperationException();
   }

   @Override
   public ByteBuf booleanResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, boolean result) {
      throw new UnsupportedOperationException();
   }

   @Override
   public ByteBuf unsignedLongResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, long value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public ByteBuf valueWithVersionResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, byte[] value, long version) {
      ByteBuf buf = writeHeader(header, server, alloc, OperationStatus.Success);
      buf.writeLong(version);
      ExtendedByteBuf.writeRangedBytes(value, buf);
      return buf;
   }

   @Override
   public ByteBuf longResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, long value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public OperationStatus errorStatus(Throwable t) {
      return OperationStatus.ServerError;
   }

   private ByteBuf writeHeader(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, OperationStatus status) {
      ByteBuf buf = alloc.ioBuffer();
      AbstractTopologyResponse topologyResp = getTopologyResponse(header, server.getAddressCache(), server);
      buf.writeByte(Constants.MAGIC_RES);
      ExtendedByteBuf.writeUnsignedLong(header.messageId, buf);
      buf.writeByte(header.op.getResponseOpCode());
      buf.writeByte(status.getCode());
      if (topologyResp != null) {
         if (topologyResp instanceof TopologyAwareResponse) {
            TopologyAwareResponse tar = (TopologyAwareResponse) topologyResp;
            if (header.clientIntel == Constants.INTELLIGENCE_TOPOLOGY_AWARE)
               writeTopologyUpdate(tar, buf);
            else
               writeLimitedHashTopologyUpdate(tar, buf);
         } else if (topologyResp instanceof AbstractHashDistAwareResponse) {
            writeHashTopologyUpdate((AbstractHashDistAwareResponse) topologyResp, server, header, buf);
         } else {
            throw new IllegalArgumentException("Unsupported response instance: " + topologyResp);
         }
      } else {
         writeNoTopologyUpdate(buf);
      }
      return buf;
   }

   private AbstractTopologyResponse getTopologyResponse(HotRodHeader header, Cache<Address, ServerAddress> addressCache, HotRodServer server) {
      // If clustered, set up a cache for topology information
      if (addressCache != null) {
         switch (header.clientIntel) {
            case Constants.INTELLIGENCE_TOPOLOGY_AWARE:
            case Constants.INTELLIGENCE_HASH_DISTRIBUTION_AWARE:
               // Use the request cache's topology id as the HotRod topologyId.
               AdvancedCache cache = server.getCacheInstance(null, header.cacheName, addressCache.getCacheManager(), false, true);
               RpcManager rpcManager = cache.getRpcManager();
               // Only send a topology update if the cache is clustered
               int currentTopologyId = rpcManager == null ? Constants.DEFAULT_TOPOLOGY_ID : rpcManager.getTopologyId();
               // AND if the client's topology id is smaller than the server's topology id
               if (currentTopologyId >= Constants.DEFAULT_TOPOLOGY_ID && header.topologyId < currentTopologyId)
                  return generateTopologyResponse(header, addressCache, server, currentTopologyId);
         }
      }
      return null;
   }

   private AbstractTopologyResponse generateTopologyResponse(HotRodHeader header, Cache<Address, ServerAddress> addressCache,
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
      AdvancedCache<byte[], byte[]> cache = server.getCacheInstance(null, header.cacheName, addressCache.getCacheManager(), false, true);
      List<Address> cacheMembers = cache.getRpcManager().getMembers();

      int responseTopologyId = currentTopologyId;
      if (!addressCache.keySet().containsAll(cacheMembers)) {
         // At least one cache member is missing from the topology cache
         int clientTopologyId = header.topologyId;
         if (currentTopologyId - clientTopologyId < 2) {
            // Postpone topology update
            return null;
         } else {
            // Send partial topology update
            responseTopologyId -= 1;
         }
      }

      Configuration config = cache.getCacheConfiguration();
      if (header.clientIntel == Constants.INTELLIGENCE_TOPOLOGY_AWARE || !config.clustering().cacheMode().isDistributed()) {
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

   void writeHashTopologyUpdate(AbstractHashDistAwareResponse h, HotRodServer server, HotRodHeader header, ByteBuf buffer) {
      AdvancedCache<byte[], byte[]> cache = server.getCacheInstance(null, header.cacheName, server.getCacheManager(), false, true);
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

   private List<Integer> extractSegmentEndHashes(KeyPartitioner keyPartitioner) {
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

   private void writeTopologyUpdate(TopologyAwareResponse t, ByteBuf buffer) {
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

   private void writeNoTopologyUpdate(ByteBuf buffer) {
      log.trace("Write topology response header with no change");
      buffer.writeByte(0);
   }

   void writeCommonHashTopologyHeader(ByteBuf buffer, int viewId,
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
