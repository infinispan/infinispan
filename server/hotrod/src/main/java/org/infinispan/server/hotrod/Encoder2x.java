package org.infinispan.server.hotrod;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.transport.ExtendedByteBuf;
import org.infinispan.topology.CacheTopology;

import io.netty.buffer.ByteBuf;

/**
 * @author Galder Zamarreño
 */
class Encoder2x implements VersionedEncoder {
   private static final Log log = LogFactory.getLog(Encoder2x.class, Log.class);
   private static final boolean isTrace = log.isTraceEnabled();

   @Override
   public void writeEvent(Events.Event e, ByteBuf buf) {
      if (isTrace)
         log.tracef("Write event %s", e);

      buf.writeByte(Constants.MAGIC_RES);
      ExtendedByteBuf.writeUnsignedLong(e.messageId, buf);
      buf.writeByte(e.op.getResponseOpCode());
      buf.writeByte(OperationStatus.Success.getCode());
      buf.writeByte(0); // no topology change
      ExtendedByteBuf.writeRangedBytes(e.listenerId, buf);
      e.writeEvent(buf);
   }

   @Override
   public void writeHeader(Response r, ByteBuf buf, Cache<Address, ServerAddress> addressCache, HotRodServer server) {
      // Sometimes an error happens before we have added the cache to the knownCaches/knownCacheConfigurations map
      // If that happens, we pretend the cache is LOCAL and we skip the topology update
      String cacheName = r.cacheName.isEmpty() ? server.getConfiguration().defaultCacheName() : r.cacheName;
      ComponentRegistry cr = server.getCacheRegistry(cacheName);
      Configuration configuration = server.getCacheConfiguration(cacheName);
      CacheMode cacheMode = configuration == null ? CacheMode.LOCAL : configuration.clustering().cacheMode();

      CacheTopology cacheTopology = cacheMode.isClustered() ? cr.getStateTransferManager().getCacheTopology() : null;
      Optional<AbstractTopologyResponse> newTopology = getTopologyResponse(r, addressCache, cacheMode, cacheTopology);


      buf.writeByte(Constants.MAGIC_RES);
      ExtendedByteBuf.writeUnsignedLong(r.messageId, buf);
      buf.writeByte(r.operation.getResponseOpCode());
      writeStatus(r, buf, server, configuration);
      if (newTopology.isPresent()) {
         AbstractTopologyResponse topology = newTopology.get();
         if (topology instanceof TopologyAwareResponse) {
            writeTopologyUpdate((TopologyAwareResponse) topology, buf);
            if (r.clientIntel == Constants.INTELLIGENCE_HASH_DISTRIBUTION_AWARE)
               writeEmptyHashInfo(topology, buf);
         } else if (topology instanceof HashDistAware20Response) {
            writeHashTopologyUpdate((HashDistAware20Response) topology, cacheTopology, buf);
         } else {
            throw new IllegalArgumentException("Unsupported response: " + topology);
         }
      } else {
         if (isTrace) log.trace("Write topology response header with no change");
         buf.writeByte(0);
      }
   }

   private void writeStatus(Response r, ByteBuf buf, HotRodServer server, Configuration cfg) {
      if (server == null || Constants.isVersionPre24(r.version))
         buf.writeByte(r.status.getCode());
      else {
         OperationStatus st = OperationStatus.withCompatibility(r.status, cfg.compatibility().enabled());
         buf.writeByte(st.getCode());
      }
   }

   private void writeTopologyUpdate(TopologyAwareResponse t, ByteBuf buffer) {
      Map<Address, ServerAddress> topologyMap = t.serverEndpointsMap;
      if (topologyMap.isEmpty()) {
         log.noMembersInTopology();
         buffer.writeByte(0); // Topology not changed
      } else {
         if (isTrace) log.tracef("Write topology change response header %s", t);
         buffer.writeByte(1); // Topology changed
         ExtendedByteBuf.writeUnsignedInt(t.topologyId, buffer);
         ExtendedByteBuf.writeUnsignedInt(topologyMap.size(), buffer);
         for (ServerAddress address : topologyMap.values()) {
            ExtendedByteBuf.writeString(address.getHost(), buffer);
            ExtendedByteBuf.writeUnsignedShort(address.getPort(), buffer);
         }
      }
   }

   private void writeEmptyHashInfo(AbstractTopologyResponse t, ByteBuf buffer) {
      if (isTrace) log.tracef("Return limited hash distribution aware header because the client %s doesn't ", t);
      buffer.writeByte(0); // Hash Function Version
      ExtendedByteBuf.writeUnsignedInt(t.numSegments, buffer);
   }

   private void writeHashTopologyUpdate(HashDistAware20Response h, CacheTopology cacheTopology, ByteBuf buf) {
      // Calculate members first, in case there are no members
      ConsistentHash ch = cacheTopology.getReadConsistentHash();
      Map<Address, ServerAddress> members = h.serverEndpointsMap.entrySet().stream().filter(e ->
            ch.getMembers().contains(e.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      if (isTrace) {
         log.trace("Topology cache contains: " + h.serverEndpointsMap);
         log.trace("After read consistent hash filter, members are: " + members);
      }

      if (members.isEmpty()) {
         log.noMembersInHashTopology(ch, h.serverEndpointsMap.toString());
         buf.writeByte(0); // Topology not changed
      } else {
         if (isTrace) log.tracef("Write hash distribution change response header %s", h);
         buf.writeByte(1); // Topology changed
         ExtendedByteBuf.writeUnsignedInt(h.topologyId, buf); // Topology ID

         // Write members
         AtomicInteger indexCount = new AtomicInteger(-1);
         ExtendedByteBuf.writeUnsignedInt(members.size(), buf);
         Map<Address, Integer> indexedMembers = new HashMap<>();
         members.forEach((addr, serverAddr) -> {
            ExtendedByteBuf.writeString(serverAddr.getHost(), buf);
            ExtendedByteBuf.writeUnsignedShort(serverAddr.getPort(), buf);
            indexCount.incrementAndGet();
            indexedMembers.put(addr, indexCount.get()); // easier indexing
         });

         // Write segment information
         int numSegments = ch.getNumSegments();
         buf.writeByte(h.hashFunction); // Hash function
         ExtendedByteBuf.writeUnsignedInt(numSegments, buf);

         for (int segmentId = 0; segmentId < numSegments; ++segmentId) {
            List<Address> owners = ch.locateOwnersForSegment(segmentId).stream().filter(members::containsKey).collect(Collectors.toList());
            int ownersSize = owners.size();
            if (ownersSize == 0) {
               // When sending partial updates, number of owners could be 0,
               // in which case just take the first member in the list.
               buf.writeByte(1);
               ExtendedByteBuf.writeUnsignedInt(0, buf);
            } else {
               buf.writeByte(ownersSize);
               owners.forEach(ownerAddr -> {
                  Integer index = indexedMembers.get(ownerAddr);
                  if (index != null) {
                     ExtendedByteBuf.writeUnsignedInt(index, buf);
                  }
               });
            }
         }
      }
   }

   private Optional<AbstractTopologyResponse> getTopologyResponse(Response r, Cache<Address, ServerAddress> addressCache,
                                                                  CacheMode cacheMode, CacheTopology cacheTopology) {
      // If clustered, set up a cache for topology information
      if (addressCache != null) {
         switch (r.clientIntel) {
            case Constants.INTELLIGENCE_TOPOLOGY_AWARE:
            case Constants.INTELLIGENCE_HASH_DISTRIBUTION_AWARE: {
               // Only send a topology update if the cache is clustered
               if (cacheMode.isClustered()) {
                  // Use the request cache's topology id as the HotRod topologyId.
                  int currentTopologyId = cacheTopology.getTopologyId();
                  // AND if the client's topology id is smaller than the server's topology id
                  if (r.topologyId < currentTopologyId)
                     return generateTopologyResponse(r, addressCache, cacheMode, cacheTopology);
               }
            }
         }
      }
      return Optional.empty();
   }

   private Optional<AbstractTopologyResponse> generateTopologyResponse(Response r,
                                                                       Cache<Address, ServerAddress> addressCache, CacheMode cacheMode, CacheTopology cacheTopology) {
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
      int currentTopologyId = cacheTopology.getTopologyId();
      List<Address> cacheMembers = cacheTopology.getMembers();
      Map<Address, ServerAddress> serverEndpoints = new HashMap<>();
      addressCache.forEach(serverEndpoints::put);

      int topologyId = currentTopologyId;

      if (isTrace) {
         log.tracef("Check for partial topologies: members=%s, endpoints=%s, client-topology=%s, server-topology=%s",
               cacheMembers, cacheMembers, r.topologyId, topologyId);
      }

      if (!serverEndpoints.keySet().containsAll(cacheMembers)) {
         // At least one cache member is missing from the topology cache
         int clientTopologyId = r.topologyId;
         if (currentTopologyId - clientTopologyId < 2) {
            if (isTrace) log.trace("Postpone topology update");
            return Optional.empty(); // Postpone topology update
         } else {
            // Send partial topology update
            topologyId -= 1;
            if (isTrace) log.tracef("Send partial topology update with topology id %s", topologyId);
         }
      }

      if (r.clientIntel == Constants.INTELLIGENCE_HASH_DISTRIBUTION_AWARE && !cacheMode.isInvalidation()) {
         int numSegments = cacheTopology.getReadConsistentHash().getNumSegments();
         return Optional.of(new HashDistAware20Response(topologyId, serverEndpoints, numSegments,
               Constants.DEFAULT_CONSISTENT_HASH_VERSION));
      } else {
         return Optional.of(new TopologyAwareResponse(topologyId, serverEndpoints, 0));
      }
   }

   @Override
   public void writeResponse(Response response, ByteBuf buf, EmbeddedCacheManager cacheManager, HotRodServer server) {
      switch(response.operation) {
         case GET: {
            GetResponse r = (GetResponse) response;
            if (r.status == OperationStatus.Success) ExtendedByteBuf.writeRangedBytes(r.data, buf);
            break;
         }
         case GET_WITH_METADATA: {
            GetWithMetadataResponse r = (GetWithMetadataResponse) response;
            if (r.status == OperationStatus.Success) {
               writeMetadata(r.lifespan, r.maxIdle, r.created, r.lastUsed, r.dataVersion, buf);
               ExtendedByteBuf.writeRangedBytes(r.data, buf);
            }
            break;
         }
         case GET_WITH_VERSION: {
            GetWithVersionResponse r = (GetWithVersionResponse) response;
            if (r.status == OperationStatus.Success) {
               buf.writeLong(r.dataVersion);
               ExtendedByteBuf.writeRangedBytes(r.data, buf);
            }
            break;
         }
         case GET_STREAM: {
            GetStreamResponse r = (GetStreamResponse) response;
            if (r.status == OperationStatus.Success) {
               writeMetadata(r.lifespan, r.maxIdle, r.created, r.lastUsed, r.dataVersion, buf);
               ExtendedByteBuf.writeRangedBytes(r.data, r.offset, buf);
            }
            break;
         }
         case PUT:
         case PUT_IF_ABSENT:
         case REPLACE:
         case REPLACE_IF_UNMODIFIED:
         case REMOVE:
         case REMOVE_IF_UNMODIFIED: {
            if (response instanceof ResponseWithPrevious) {
               ResponseWithPrevious r = (ResponseWithPrevious) response;
               if (!r.previous.isPresent())
                  ExtendedByteBuf.writeUnsignedInt(0, buf);
               else
                  ExtendedByteBuf.writeRangedBytes(r.previous.get(), buf);
            }
            break;
         }
         case STATS: {
            StatsResponse r = (StatsResponse) response;
            ExtendedByteBuf.writeUnsignedInt(r.stats.size(), buf);
            for (Map.Entry<String, String> stat : r.stats.entrySet()) {
               ExtendedByteBuf.writeString(stat.getKey(), buf);
               ExtendedByteBuf.writeString(stat.getValue(), buf);
            }
            break;
         }
         case PING:
         case CLEAR:
         case CONTAINS_KEY:
         case PUT_ALL:
         case PUT_STREAM:
         case ITERATION_END:
         case ADD_CLIENT_LISTENER:
         case REMOVE_CLIENT_LISTENER:
            // Empty response
            break;
         case SIZE: {
            SizeResponse r = (SizeResponse) response;
            ExtendedByteBuf.writeUnsignedLong(r.size, buf);
            break;
         }
         case AUTH_MECH_LIST: {
            AuthMechListResponse r = (AuthMechListResponse) response;
            ExtendedByteBuf.writeUnsignedInt(r.mechs.size(), buf);
            r.mechs.forEach(s -> ExtendedByteBuf.writeString(s, buf));
            break;
         }
         case AUTH: {
            AuthResponse r = (AuthResponse) response;
            if (r.challenge != null) {
               buf.writeBoolean(false);
               ExtendedByteBuf.writeRangedBytes(r.challenge, buf);
            } else {
               buf.writeBoolean(true);
               ExtendedByteBuf.writeUnsignedInt(0, buf);
            }
            break;
         }
         case EXEC: {
            ExecResponse r = (ExecResponse) response;
            ExtendedByteBuf.writeRangedBytes(r.result, buf);
            break;
         }
         case BULK_GET: {
            BulkGetResponse r = (BulkGetResponse) response;
            if (isTrace) log.trace("About to respond to bulk get request");
            if (r.status == OperationStatus.Success) {
               try (CloseableIterator<Map.Entry<byte[], byte[]>> iterator = r.entries.iterator()) {
                  int max = Integer.MAX_VALUE;
                  if (r.count != 0) {
                     if (isTrace) log.tracef("About to write (max) %d messages to the client", r.count);
                     max = r.count;
                  }
                  int count = 0;
                  while (iterator.hasNext() && count < max) {
                     Map.Entry<byte[], byte[]> entry = iterator.next();
                     buf.writeByte(1); // Not done
                     ExtendedByteBuf.writeRangedBytes(entry.getKey(), buf);
                     ExtendedByteBuf.writeRangedBytes(entry.getValue(), buf);
                     count++;
                  }
                  buf.writeByte(0); // Done
               }
            }
            break;
         }
         case BULK_GET_KEYS: {
            BulkGetKeysResponse r = (BulkGetKeysResponse) response;
            if (r.status == OperationStatus.Success) {
               r.iterator.forEachRemaining(key -> {
                  buf.writeByte(1); // Not done
                  ExtendedByteBuf.writeRangedBytes(key, buf);
               });
               buf.writeByte(0); // Done
            }
            break;
         }
         case QUERY: {
            QueryResponse r = (QueryResponse) response;
            ExtendedByteBuf.writeRangedBytes(r.result, buf);
            break;
         }
         case ITERATION_START: {
            IterationStartResponse r = (IterationStartResponse) response;
            ExtendedByteBuf.writeString(r.iterationId, buf);
            break;
         }
         case ITERATION_NEXT: {
            IterationNextResponse r = (IterationNextResponse) response;
            ExtendedByteBuf.writeRangedBytes(r.iterationResult.segmentsToBytes(), buf);
            List<CacheEntry> entries = r.iterationResult.getEntries();
            ExtendedByteBuf.writeUnsignedInt(entries.size(), buf);
            Optional<Integer> projectionLength = projectionInfo(entries, r.version);
            projectionLength.ifPresent(i -> ExtendedByteBuf.writeUnsignedInt(i, buf));
            entries.forEach(cacheEntry -> {
               if (Constants.isVersionPost24(r.version)) {
                  if (r.iterationResult.isMetadata()) {
                     buf.writeByte(1);
                     InternalCacheEntry ice = (InternalCacheEntry) cacheEntry;
                     int lifespan = ice.getLifespan() < 0 ? -1 : (int) (ice.getLifespan() / 1000);
                     int maxIdle = ice.getMaxIdle() < 0 ? -1 : (int) (ice.getMaxIdle() / 1000);
                     long lastUsed = ice.getLastUsed();
                     long created = ice.getCreated();
                     long dataVersion = CacheDecodeContext.extractVersion(ice.getMetadata().version());
                     writeMetadata(lifespan, maxIdle, created, lastUsed, dataVersion, buf);
                  } else {
                     buf.writeByte(0);
                  }
               }
               Object key = cacheEntry.getKey();
               Object value = cacheEntry.getValue();
               if (r.iterationResult.isCompatEnabled()) {
                  key = r.iterationResult.unbox(key);
                  value = r.iterationResult.unbox(value);
               }
               ExtendedByteBuf.writeRangedBytes((byte[]) key, buf);
               if (value instanceof Object[]) {
                  for (Object o : (Object[]) value) {
                     ExtendedByteBuf.writeRangedBytes((byte[]) o, buf);
                  }
               } else if (value instanceof byte[]) {
                  ExtendedByteBuf.writeRangedBytes((byte[]) value, buf);
               } else {
                  throw new IllegalArgumentException("Unsupported type passed: " + value.getClass());
               }
            });
            break;
         }
         case GET_ALL: {
            GetAllResponse r = (GetAllResponse) response;
            if (r.status == OperationStatus.Success) {
               ExtendedByteBuf.writeUnsignedInt(r.entries.size(), buf);
               r.entries.forEach((k, v) -> {
                  ExtendedByteBuf.writeRangedBytes(k, buf);
                  ExtendedByteBuf.writeRangedBytes(v, buf);
               });
            }
            break;
         }
         case ERROR: {
            ErrorResponse r = (ErrorResponse) response;
            ExtendedByteBuf.writeString(r.msg, buf);
            break;
         }
         case CACHE_ENTRY_CREATED_EVENT:
         case CACHE_ENTRY_MODIFIED_EVENT:
         case CACHE_ENTRY_REMOVED_EVENT:
         case CACHE_ENTRY_EXPIRED_EVENT:
            throw new UnsupportedOperationException(response.toString());
         case COMMIT_TX:
         case PREPARE_TX:
         case ROLLBACK_TX:
            TransactionResponse txRsp = (TransactionResponse) response;
            if (txRsp.status == OperationStatus.Success) {
               buf.writeInt(txRsp.xaReturnCode);
            }
            break;
         default:
            throw new UnsupportedOperationException(response.toString());
      }
   }

   static void writeMetadata(int lifespan, int maxIdle, long created, long lastUsed, long dataVersion, ByteBuf buf) {
      int flags = (lifespan < 0 ? Constants.INFINITE_LIFESPAN : 0) + (maxIdle < 0 ? Constants.INFINITE_MAXIDLE : 0);
      buf.writeByte(flags);
      if (lifespan >= 0) {
         buf.writeLong(created);
         ExtendedByteBuf.writeUnsignedInt(lifespan, buf);
      }
      if (maxIdle >= 0) {
         buf.writeLong(lastUsed);
         ExtendedByteBuf.writeUnsignedInt(maxIdle, buf);
      }
      buf.writeLong(dataVersion);
   }

   static Optional<Integer> projectionInfo(List<CacheEntry> entries, byte version) {
      if (!entries.isEmpty()) {
         CacheEntry entry = entries.get(0);
         if (entry.getValue() instanceof Object[]) {
            return Optional.of(((Object[]) entry.getValue()).length);
         } else if (!Constants.isVersionPre24(version)) {
            return Optional.of(1);
         }
      }
      return Optional.empty();
   }
}
