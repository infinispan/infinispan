package org.infinispan.server.hotrod;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.counter.util.EncodeUtil.encodeConfiguration;
import static org.infinispan.server.core.transport.VInt.write;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.writeString;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.writeUnsignedInt;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.writeUnsignedLong;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.writeXid;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.PrivilegedActionException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.MediaTypeIds;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.impl.CounterModuleLifecycle;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.core.transport.VInt;
import org.infinispan.server.hotrod.counter.listener.ClientCounterEvent;
import org.infinispan.server.hotrod.iteration.IterableIterationResult;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.transport.ExtendedByteBuf;
import org.infinispan.stats.ClusterCacheStats;
import org.infinispan.stats.Stats;
import org.infinispan.topology.CacheTopology;
import org.jgroups.SuspectedException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;

/**
 * @author Galder Zamarreño
 */
class Encoder2x implements VersionedEncoder {
   private static final Log log = LogFactory.getLog(Encoder2x.class, Log.class);
   private static final int topologyCheckInterval;

   static {
      String intervalStr = SecurityActions.getSystemProperty("infinispan.server.topology-check-interval");
      if (intervalStr != null) {
         topologyCheckInterval = Integer.parseInt(intervalStr);
      } else {
         // Default interval of 5 seconds
         topologyCheckInterval = 5_000;
      }
   }

   private static final Encoder2x INSTANCE = new Encoder2x();

   private final ConcurrentMap<ChannelId, LastKnownClientTopologyId> lastKnownTopologyIds = topologyCheckInterval > 0 ? new ConcurrentHashMap<>() : null;

   private static class LastKnownClientTopologyId {
      private final int topologyId;
      private final long timeCaptured;

      private LastKnownClientTopologyId(int topologyId, long timeCaptured) {
         this.topologyId = topologyId;
         this.timeCaptured = timeCaptured;
      }
   }

   protected Encoder2x() {
   }

   public static Encoder2x instance() {
      return INSTANCE;
   }

   @Override
   public void writeEvent(Events.Event e, ByteBuf buf) {
      writeHeaderNoTopology(buf, e.messageId, e.op);
      ExtendedByteBuf.writeRangedBytes(e.listenerId, buf);
      e.writeEvent(buf);
   }

   @Override
   public ByteBuf authResponse(HotRodHeader header, HotRodServer server, Channel channel, byte[] challenge) {
      ByteBuf buf = writeHeader(header, server, channel, OperationStatus.Success);
      if (challenge != null) {
         buf.writeBoolean(false);
         ExtendedByteBuf.writeRangedBytes(challenge, buf);
      } else {
         buf.writeBoolean(true);
         ExtendedByteBuf.writeUnsignedInt(0, buf);
      }
      return buf;
   }

   @Override
   public ByteBuf authMechListResponse(HotRodHeader header, HotRodServer server, Channel channel, Set<String> mechs) {
      ByteBuf buf = writeHeader(header, server, channel, OperationStatus.Success);
      ExtendedByteBuf.writeUnsignedInt(mechs.size(), buf);
      for (String s : mechs) {
         ExtendedByteBuf.writeString(s, buf);
      }
      return buf;
   }

   @Override
   public ByteBuf notExecutedResponse(HotRodHeader header, HotRodServer server, Channel channel, CacheEntry<byte[], byte[]> prev) {
      return valueResponse(header, server, channel, OperationStatus.NotExecutedWithPrevious, prev);
   }

   @Override
   public ByteBuf notExistResponse(HotRodHeader header, HotRodServer server, Channel channel) {
      return emptyResponse(header, server, channel, OperationStatus.KeyDoesNotExist);
   }

   @Override
   public ByteBuf valueResponse(HotRodHeader header, HotRodServer server, Channel channel, OperationStatus status, byte[] prev) {
      ByteBuf buf = writeHeader(header, server, channel, status);
      if (prev == null) {
         ExtendedByteBuf.writeUnsignedInt(0, buf);
      } else {
         ExtendedByteBuf.writeRangedBytes(prev, buf);
      }
      if (log.isTraceEnabled()) {
         log.tracef("Write response to %s messageId=%d status=%s prev=%s", header.op, header.messageId, status, Util.printArray(prev));
      }
      return buf;
   }

   @Override
   public ByteBuf valueResponse(HotRodHeader header, HotRodServer server, Channel channel, OperationStatus status, CacheEntry<byte[], byte[]> prev) {
      return valueResponse(header, server, channel, status, prev != null ? prev.getValue() : null);
   }

   @Override
   public ByteBuf successResponse(HotRodHeader header, HotRodServer server, Channel channel, CacheEntry<byte[], byte[]> result) {
      return valueResponse(header, server, channel, OperationStatus.SuccessWithPrevious, result);
   }

   @Override
   public ByteBuf errorResponse(HotRodHeader header, HotRodServer server, Channel channel, String message, OperationStatus status) {
      ByteBuf buf = writeHeader(header, server, channel, status);
      ExtendedByteBuf.writeString(message, buf);
      return buf;
   }

   @Override
   public ByteBuf bulkGetResponse(HotRodHeader header, HotRodServer server, Channel channel, int size, CacheSet<Map.Entry<byte[], byte[]>> entries) {
      ByteBuf buf = writeHeader(header, server, channel, OperationStatus.Success);
      try (CloseableIterator<Map.Entry<byte[], byte[]>> iterator = entries.iterator()) {
         int max = Integer.MAX_VALUE;
         if (size != 0) {
            if (log.isTraceEnabled()) log.tracef("About to write (max) %d messages to the client", size);
            max = size;
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
      return buf;
   }

   @Override
   public ByteBuf emptyResponse(HotRodHeader header, HotRodServer server, Channel channel, OperationStatus status) {
      return writeHeader(header, server, channel, status);
   }

   @Override
   public ByteBuf pingResponse(HotRodHeader header, HotRodServer server, Channel channel, OperationStatus status) {
      if (HotRodVersion.HOTROD_30.isAtLeast(header.version)) {
         ByteBuf buf = writeHeader(header, server, channel, status, true);
         buf.writeByte(HotRodVersion.LATEST.getVersion());
         ExtendedByteBuf.writeUnsignedInt(HotRodOperation.REQUEST_COUNT, buf);
         for (HotRodOperation op : HotRodOperation.VALUES) {
            // We only include request ops
            if (op.getRequestOpCode() > 0) {
               buf.writeShort(op.getRequestOpCode());
            }
         }
         return buf;
      } else {
         return writeHeader(header, server, channel, status, true);
      }
   }

   @Override
   public ByteBuf statsResponse(HotRodHeader header, HotRodServer server, Channel channel, Stats stats,
         NettyTransport transport, ClusterCacheStats clusterCacheStats) {
      ByteBuf buf = writeHeader(header, server, channel, OperationStatus.Success);
      int numStats = 11;
      if (transport != null) {
         numStats += 2;
      }
      if (HotRodVersion.HOTROD_24.isAtLeast(header.version)) {
         if (clusterCacheStats != null) {
            numStats += 9;
         }
      } else {
         // Ignore the cluster stats
         clusterCacheStats = null;
      }

      ExtendedByteBuf.writeUnsignedInt(numStats, buf);
      writePair(buf, "timeSinceStart", String.valueOf(stats.getTimeSinceStart()));
      writePair(buf, "currentNumberOfEntries", String.valueOf(stats.getCurrentNumberOfEntries()));
      writePair(buf, "totalNumberOfEntries", String.valueOf(stats.getTotalNumberOfEntries()));
      writePair(buf, "stores", String.valueOf(stats.getStores()));
      writePair(buf, "retrievals", String.valueOf(stats.getRetrievals()));
      writePair(buf, "hits", String.valueOf(stats.getHits()));
      writePair(buf, "misses", String.valueOf(stats.getMisses()));
      writePair(buf, "removeHits", String.valueOf(stats.getRemoveHits()));
      writePair(buf, "removeMisses", String.valueOf(stats.getRemoveMisses()));
      writePair(buf, "approximateEntries", String.valueOf(stats.getApproximateEntries()));
      writePair(buf, "approximateEntriesUnique", String.valueOf(stats.getApproximateEntriesUnique()));

      if (transport != null) {
         writePair(buf, "totalBytesRead", String.valueOf(transport.getTotalBytesRead()));
         writePair(buf, "totalBytesWritten", String.valueOf(transport.getTotalBytesWritten()));
      }

      if (clusterCacheStats != null) {
         writePair(buf, "globalCurrentNumberOfEntries", String.valueOf(clusterCacheStats.getCurrentNumberOfEntries()));
         writePair(buf, "globalStores", String.valueOf(clusterCacheStats.getStores()));
         writePair(buf, "globalRetrievals", String.valueOf(clusterCacheStats.getRetrievals()));
         writePair(buf, "globalHits", String.valueOf(clusterCacheStats.getHits()));
         writePair(buf, "globalMisses", String.valueOf(clusterCacheStats.getMisses()));
         writePair(buf, "globalRemoveHits", String.valueOf(clusterCacheStats.getRemoveHits()));
         writePair(buf, "globalRemoveMisses", String.valueOf(clusterCacheStats.getRemoveMisses()));
         writePair(buf, "globalApproximateEntries", String.valueOf(clusterCacheStats.getApproximateEntries()));
         writePair(buf, "globalApproximateEntriesUnique", String.valueOf(clusterCacheStats.getApproximateEntriesUnique()));
      }
      return buf;
   }

   private void writePair(ByteBuf buf, String key, String value) {
      ExtendedByteBuf.writeString(key, buf);
      ExtendedByteBuf.writeString(value, buf);
   }

   @Override
   public ByteBuf valueWithVersionResponse(HotRodHeader header, HotRodServer server, Channel channel, byte[] value, long version) {
      ByteBuf buf = writeHeader(header, server, channel, OperationStatus.Success);
      buf.writeLong(version);
      ExtendedByteBuf.writeRangedBytes(value, buf);
      return buf;
   }


   @Override
   public ByteBuf getWithMetadataResponse(HotRodHeader header, HotRodServer server, Channel channel, CacheEntry<byte[], byte[]> entry) {
      return writeMetadataResponse(header, server, channel, OperationStatus.Success, entry);
   }

   protected ByteBuf writeMetadataResponse(HotRodHeader header, HotRodServer server, Channel channel, OperationStatus status, CacheEntry<byte[], byte[]> entry) {
      ByteBuf buf = writeHeader(header, server, channel, status);
      MetadataUtils.writeMetadata(MetadataUtils.extractLifespan(entry), MetadataUtils.extractMaxIdle(entry),
            MetadataUtils.extractCreated(entry), MetadataUtils.extractLastUsed(entry), MetadataUtils.extractVersion(entry), buf);
      ExtendedByteBuf.writeRangedBytes(entry != null ? entry.getValue() : Util.EMPTY_BYTE_ARRAY, buf);
      return buf;
   }

   @Override
   public ByteBuf getStreamResponse(HotRodHeader header, HotRodServer server, Channel channel, int offset, CacheEntry<byte[], byte[]> entry) {
      ByteBuf buf = writeHeader(header, server, channel, OperationStatus.Success);
      MetadataUtils.writeMetadata(MetadataUtils.extractLifespan(entry), MetadataUtils.extractMaxIdle(entry),
            MetadataUtils.extractCreated(entry), MetadataUtils.extractLastUsed(entry), MetadataUtils.extractVersion(entry), buf);
      ExtendedByteBuf.writeRangedBytes(entry.getValue(), offset, buf);
      return buf;
   }

   @Override
   public ByteBuf getAllResponse(HotRodHeader header, HotRodServer server, Channel channel, Map<byte[], byte[]> entries) {
      ByteBuf buf = writeHeader(header, server, channel, OperationStatus.Success);
      ExtendedByteBuf.writeUnsignedInt(entries.size(), buf);
      for (Map.Entry<byte[], byte[]> entry : entries.entrySet()) {
         ExtendedByteBuf.writeRangedBytes(entry.getKey(), buf);
         ExtendedByteBuf.writeRangedBytes(entry.getValue(), buf);
      }
      return buf;
   }

   @Override
   public ByteBuf bulkGetKeysResponse(HotRodHeader header, HotRodServer server, Channel channel, CloseableIterator<byte[]> iterator) {
      ByteBuf buf = writeHeader(header, server, channel, OperationStatus.Success);
      while (iterator.hasNext()) {
         buf.writeByte(1);
         ExtendedByteBuf.writeRangedBytes(iterator.next(), buf);
      }
      buf.writeByte(0);
      return buf;
   }

   @Override
   public ByteBuf iterationStartResponse(HotRodHeader header, HotRodServer server, Channel channel, String iterationId) {
      ByteBuf buf = writeHeader(header, server, channel, OperationStatus.Success);
      ExtendedByteBuf.writeString(iterationId, buf);
      return buf;
   }

   @Override
   public ByteBuf iterationNextResponse(HotRodHeader header, HotRodServer server, Channel channel, IterableIterationResult iterationResult) {
      ByteBuf buf = writeHeader(header, server, channel, iterationResult.getStatusCode());
      ExtendedByteBuf.writeRangedBytes(iterationResult.segmentsToBytes(), buf);
      List<CacheEntry> entries = iterationResult.getEntries();
      ExtendedByteBuf.writeUnsignedInt(entries.size(), buf);
      Optional<Integer> projectionLength = projectionInfo(entries, header.version);
      projectionLength.ifPresent(i -> ExtendedByteBuf.writeUnsignedInt(i, buf));
      for (CacheEntry cacheEntry : entries) {
         if (HotRodVersion.HOTROD_25.isAtLeast(header.version)) {
            if (iterationResult.isMetadata()) {
               buf.writeByte(1);
               InternalCacheEntry ice = (InternalCacheEntry) cacheEntry;
               int lifespan = ice.getLifespan() < 0 ? -1 : (int) (ice.getLifespan() / 1000);
               int maxIdle = ice.getMaxIdle() < 0 ? -1 : (int) (ice.getMaxIdle() / 1000);
               long lastUsed = ice.getLastUsed();
               long created = ice.getCreated();
               long dataVersion = MetadataUtils.extractVersion(ice);
               MetadataUtils.writeMetadata(lifespan, maxIdle, created, lastUsed, dataVersion, buf);
            } else {
               buf.writeByte(0);
            }
         }
         Object key = iterationResult.getResultFunction().apply(cacheEntry.getKey());
         Object value = cacheEntry.getValue();
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
      }
      return buf;
   }

   @Override
   public ByteBuf counterConfigurationResponse(HotRodHeader header, HotRodServer server, Channel channel, CounterConfiguration configuration) {
      ByteBuf buf = writeHeader(header, server, channel, OperationStatus.Success);
      encodeConfiguration(configuration, buf::writeByte, buf::writeLong,
            value -> ExtendedByteBuf.writeUnsignedInt(value, buf));
      return buf;
   }

   @Override
   public ByteBuf counterNamesResponse(HotRodHeader header, HotRodServer server, Channel channel, Collection<String> counterNames) {
      ByteBuf buf = writeHeader(header, server, channel, OperationStatus.Success);
      write(buf, counterNames.size());
      for (String s : counterNames) {
         writeString(s, buf);
      }
      return buf;
   }

   @Override
   public ByteBuf multimapCollectionResponse(HotRodHeader header, HotRodServer server, Channel channel, OperationStatus status, Collection<byte[]> values) {
      ByteBuf buf = writeHeader(header, server, channel, status);
      ExtendedByteBuf.writeUnsignedInt(values.size(), buf);
      for (byte[] v : values) {
         ExtendedByteBuf.writeRangedBytes(v, buf);
      }
      return buf;
   }

   @Override
   public ByteBuf multimapEntryResponse(HotRodHeader header, HotRodServer server, Channel channel, OperationStatus status, CacheEntry<byte[], Collection<byte[]>> entry) {
      ByteBuf buf = writeHeader(header, server, channel, status);
      MetadataUtils.writeMetadata(MetadataUtils.extractLifespan(entry), MetadataUtils.extractMaxIdle(entry),
            MetadataUtils.extractCreated(entry), MetadataUtils.extractLastUsed(entry), MetadataUtils.extractVersion(entry), buf);
      Collection<byte[]> values = entry.getValue();
      if (values == null) {
         buf.writeByte(0);
      } else {
         ExtendedByteBuf.writeUnsignedInt(values.size(), buf);
         for (byte[] v : values) {
            ExtendedByteBuf.writeRangedBytes(v, buf);
         }
      }
      return buf;
   }

   @Override
   public ByteBuf booleanResponse(HotRodHeader header, HotRodServer server, Channel channel, boolean result) {
      ByteBuf buf = writeHeader(header, server, channel, OperationStatus.Success);
      buf.writeByte(result ? 1 : 0);
      return buf;
   }

   @Override
   public ByteBuf unsignedLongResponse(HotRodHeader header, HotRodServer server, Channel channel, long value) {
      ByteBuf buf = writeHeader(header, server, channel, OperationStatus.Success);
      ExtendedByteBuf.writeUnsignedLong(value, buf);
      return buf;
   }

   @Override
   public ByteBuf longResponse(HotRodHeader header, HotRodServer server, Channel channel, long value) {
      ByteBuf buf = writeHeader(header, server, channel, OperationStatus.Success);
      buf.writeLong(value);
      return buf;
   }

   @Override
   public ByteBuf transactionResponse(HotRodHeader header, HotRodServer server, Channel channel, int xaReturnCode) {
      ByteBuf buf = writeHeader(header, server, channel, OperationStatus.Success);
      buf.writeInt(xaReturnCode);
      return buf;
   }

   @Override
   public ByteBuf recoveryResponse(HotRodHeader header, HotRodServer server, Channel channel, Collection<XidImpl> xids) {
      ByteBuf buf = writeHeader(header, server, channel, OperationStatus.Success);
      writeUnsignedInt(xids.size(), buf);
      for (XidImpl xid : xids) {
         writeXid(xid, buf);
      }
      return buf;
   }

   @Override
   public OperationStatus errorStatus(Throwable t) {
      if (t instanceof SuspectException) {
         return OperationStatus.NodeSuspected;
      } else if (t instanceof IllegalLifecycleStateException) {
         return OperationStatus.IllegalLifecycleState;
      } else if (t instanceof CacheException) {
         // JGroups and remote exceptions (inside RemoteException) can come wrapped up
         Throwable cause = t.getCause() == null ? t : t.getCause();
         if (cause instanceof SuspectedException) {
            return OperationStatus.NodeSuspected;
         } else if (cause instanceof IllegalLifecycleStateException) {
            return OperationStatus.IllegalLifecycleState;
         } else if (cause instanceof InterruptedException) {
            return OperationStatus.IllegalLifecycleState;
         } else {
            return OperationStatus.ServerError;
         }
      } else if (t instanceof InterruptedException) {
         return OperationStatus.IllegalLifecycleState;
      } else if (t instanceof PrivilegedActionException) {
         return errorStatus(t.getCause());
      } else if (t instanceof SuspectedException) {
         return OperationStatus.NodeSuspected;
      } else {
         return OperationStatus.ServerError;
      }
   }

   protected ByteBuf writeHeader(HotRodHeader header, HotRodServer server, Channel channel, OperationStatus status) {
      return writeHeader(header, server, channel, status, false);
   }

   private ByteBuf writeHeader(HotRodHeader header, HotRodServer server, Channel channel, OperationStatus status, boolean sendMediaType) {
      ByteBuf buf = channel.alloc().ioBuffer();
      Cache<Address, ServerAddress> addressCache = HotRodVersion.forVersion(header.version) != HotRodVersion.UNKNOWN ?
            server.getAddressCache() : null;

      Optional<AbstractTopologyResponse> newTopology;

      MediaType keyMediaType = null;
      MediaType valueMediaType = null;
      boolean objectStorage = false;
      CacheTopology cacheTopology;

      if (header.op != HotRodOperation.ERROR) {
         if (CounterModuleLifecycle.COUNTER_CACHE_NAME.equals(header.cacheName)) {
            cacheTopology = getCounterCacheTopology(server.getCacheManager());
            newTopology = getTopologyResponse(header.clientIntel, header.topologyId, addressCache, channel,
                  server.getTimeService(), CacheMode.DIST_SYNC, cacheTopology);
         } else if (header.cacheName.isEmpty() && !server.hasDefaultCache()) {
            cacheTopology = null;
            newTopology = Optional.empty();
         } else {
            HotRodServer.ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
            Configuration configuration = cacheInfo.configuration;
            CacheMode cacheMode = configuration.clustering().cacheMode();

            cacheTopology =
                  cacheMode.isClustered() ? cacheInfo.distributionManager.getCacheTopology() : null;
            newTopology = getTopologyResponse(header.clientIntel, header.topologyId, addressCache, channel,
                  server.getTimeService(), cacheMode, cacheTopology);
            keyMediaType = configuration.encoding().keyDataType().mediaType();
            valueMediaType = configuration.encoding().valueDataType().mediaType();
            objectStorage = APPLICATION_OBJECT.match(keyMediaType);
         }
      } else {
         cacheTopology = null;
         newTopology = Optional.empty();
      }

      buf.writeByte(Constants.MAGIC_RES);
      writeUnsignedLong(header.messageId, buf);
      buf.writeByte(header.op.getResponseOpCode());
      writeStatus(header, buf, server, objectStorage, status);
      if (newTopology.isPresent()) {
         AbstractTopologyResponse topology = newTopology.get();
         if (topology instanceof TopologyAwareResponse) {
            writeTopologyUpdate((TopologyAwareResponse) topology, buf, ((InetSocketAddress) channel.localAddress()).getAddress());
            if (header.clientIntel == Constants.INTELLIGENCE_HASH_DISTRIBUTION_AWARE)
               writeEmptyHashInfo(topology, buf);
         } else if (topology instanceof HashDistAware20Response) {
            writeHashTopologyUpdate((HashDistAware20Response) topology, cacheTopology, buf, ((InetSocketAddress) channel.localAddress()).getAddress());
         } else {
            throw new IllegalArgumentException("Unsupported response: " + topology);
         }
      } else {
         if (log.isTraceEnabled()) log.trace("Write topology response header with no change");
         buf.writeByte(0);
      }
      if (sendMediaType && HotRodVersion.HOTROD_29.isAtLeast(header.version)) {
         writeMediaType(buf, keyMediaType);
         writeMediaType(buf, valueMediaType);
      }
      return buf;
   }

   @Override
   public void writeCounterEvent(ClientCounterEvent event, ByteBuf buffer) {
      writeHeaderNoTopology(buffer, 0, HotRodOperation.COUNTER_EVENT);
      event.writeTo(buffer);
   }

   private CacheTopology getCounterCacheTopology(EmbeddedCacheManager cacheManager) {
      AdvancedCache<?, ?> cache = cacheManager.getCache(CounterModuleLifecycle.COUNTER_CACHE_NAME).getAdvancedCache();
      return cache.getCacheConfiguration().clustering().cacheMode().isClustered() ?
            cache.getComponentRegistry().getDistributionManager().getCacheTopology() :
            null; //local cache
   }

   private void writeHeaderNoTopology(ByteBuf buffer, long messageId, HotRodOperation operation) {
      buffer.writeByte(Constants.MAGIC_RES);
      writeUnsignedLong(messageId, buffer);
      buffer.writeByte(operation.getResponseOpCode());
      buffer.writeByte(OperationStatus.Success.getCode());
      buffer.writeByte(0); // no topology change
   }

   private void writeStatus(HotRodHeader header, ByteBuf buf, HotRodServer server, boolean objStorage, OperationStatus status) {
      if (server == null || HotRodVersion.HOTROD_24.isOlder(header.version) || HotRodVersion.HOTROD_29.isAtLeast(header.version))
         buf.writeByte(status.getCode());
      else {
         OperationStatus st = OperationStatus.withLegacyStorageHint(status, objStorage);
         buf.writeByte(st.getCode());
      }
   }

   private void writeMediaType(ByteBuf buf, MediaType mediaType) {
      if (mediaType == null) {
         buf.writeByte(0);
      } else {
         Short id = MediaTypeIds.getId(mediaType);
         if (id != null) {
            buf.writeByte(1);
            VInt.write(buf, id);
         } else {
            buf.writeByte(2);
            ExtendedByteBuf.writeString(mediaType.toString(), buf);
         }
         Map<String, String> parameters = mediaType.getParameters();
         VInt.write(buf, parameters.size());
         parameters.forEach((key, value) -> {
            ExtendedByteBuf.writeString(key, buf);
            ExtendedByteBuf.writeString(value, buf);
         });
      }
   }

   private void writeTopologyUpdate(TopologyAwareResponse t, ByteBuf buffer, InetAddress localAddress) {
      Map<Address, ServerAddress> topologyMap = t.serverEndpointsMap;
      if (topologyMap.isEmpty()) {
         log.noMembersInTopology();
         buffer.writeByte(0); // Topology not changed
      } else {
         if (log.isTraceEnabled()) log.tracef("Write topology change response header %s", t);
         buffer.writeByte(1); // Topology changed
         ExtendedByteBuf.writeUnsignedInt(t.topologyId, buffer);
         ExtendedByteBuf.writeUnsignedInt(topologyMap.size(), buffer);
         for (ServerAddress address : topologyMap.values()) {
            ExtendedByteBuf.writeString(address.getHost(localAddress), buffer);
            ExtendedByteBuf.writeUnsignedShort(address.getPort(), buffer);
         }
      }
   }

   private void writeEmptyHashInfo(AbstractTopologyResponse t, ByteBuf buffer) {
      if (log.isTraceEnabled())
         log.tracef("Return limited hash distribution aware header because the client %s doesn't ", t);
      buffer.writeByte(0); // Hash Function Version
      ExtendedByteBuf.writeUnsignedInt(t.numSegments, buffer);
   }

   private void writeHashTopologyUpdate(HashDistAware20Response h, CacheTopology cacheTopology, ByteBuf buf, InetAddress localAddress) {
      // Calculate members first, in case there are no members
      ConsistentHash ch = cacheTopology.getReadConsistentHash();
      Map<Address, ServerAddress> members = h.serverEndpointsMap.entrySet().stream().filter(e ->
            ch.getMembers().contains(e.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      if (log.isTraceEnabled()) {
         log.trace("Topology cache contains: " + h.serverEndpointsMap);
         log.trace("After read consistent hash filter, members are: " + members);
      }

      if (members.isEmpty()) {
         log.noMembersInHashTopology(ch, h.serverEndpointsMap.toString());
         buf.writeByte(0); // Topology not changed
      } else {
         if (log.isTraceEnabled()) log.tracef("Write hash distribution change response header %s", h);
         buf.writeByte(1); // Topology changed
         ExtendedByteBuf.writeUnsignedInt(h.topologyId, buf); // Topology ID

         // Write members
         AtomicInteger indexCount = new AtomicInteger(-1);
         ExtendedByteBuf.writeUnsignedInt(members.size(), buf);
         Map<Address, Integer> indexedMembers = new HashMap<>();
         members.forEach((addr, serverAddr) -> {
            ExtendedByteBuf.writeString(serverAddr.getHost(localAddress), buf);
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

   private Optional<AbstractTopologyResponse> getTopologyResponse(short clientIntel, int topologyId, Cache<Address, ServerAddress> addressCache,
         Channel channel, TimeService timeService, CacheMode cacheMode, CacheTopology cacheTopology) {
      // If clustered, set up a cache for topology information
      if (addressCache != null) {
         switch (clientIntel) {
            case Constants.INTELLIGENCE_TOPOLOGY_AWARE:
            case Constants.INTELLIGENCE_HASH_DISTRIBUTION_AWARE: {
               // Only send a topology update if the cache is clustered
               if (cacheMode.isClustered()) {
                  // Use the request cache's topology id as the HotRod topologyId.
                  int currentTopologyId = cacheTopology.getReadConsistentHash().hashCode();
                  // AND if the client's topology id is different from the server's topology id
                  if (topologyId != currentTopologyId) {
                     Optional<AbstractTopologyResponse> respOptional =
                           generateTopologyResponse(clientIntel, topologyId, addressCache, cacheMode, cacheTopology);
                     if (lastKnownTopologyIds != null && respOptional.isPresent()) {
                        ChannelId id = channel.id();
                        LastKnownClientTopologyId lastKnown = lastKnownTopologyIds.get(id);
                        if (lastKnown != null && lastKnown.topologyId == topologyId && lastKnown.timeCaptured != -1) {
                           if (timeService.timeDuration(lastKnown.timeCaptured, TimeUnit.MILLISECONDS) > topologyCheckInterval) {
                              log.clientNotUpdatingTopology(channel.localAddress(), topologyId);
                              // Update last known topology to the current one to avoid logging a message over and over
                              lastKnownTopologyIds.replace(id, new LastKnownClientTopologyId(currentTopologyId, -1));
                           }
                        } else if (lastKnownTopologyIds.put(id, new LastKnownClientTopologyId(topologyId, timeService.time())) == null) {
                           channel.closeFuture().addListener(___ -> lastKnownTopologyIds.remove(id));
                        }
                     }
                     return respOptional;
                  }
               }
            }
         }
      }
      return Optional.empty();
   }

   private Optional<AbstractTopologyResponse> generateTopologyResponse(short clientIntel, int responseTopologyId, Cache<Address, ServerAddress> addressCache, CacheMode cacheMode, CacheTopology cacheTopology) {
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
      List<Address> cacheMembers = cacheTopology.getActualMembers();
      Map<Address, ServerAddress> serverEndpoints = new HashMap<>();
      try {
         serverEndpoints.putAll(addressCache);
      } catch (IllegalLifecycleStateException e) {
         // Address cache has been shut down, ignore the exception and don't send a topology update
         return Optional.empty();
      }

      int clientTopologyId = cacheTopology.getReadConsistentHash().hashCode();
      if (log.isTraceEnabled()) {
         log.tracef("Check for partial topologies: endpoints=%s, client-topology=%s, server-topology=%s, server-client-topology=%s",
               serverEndpoints, responseTopologyId, cacheTopology.getTopologyId(), clientTopologyId);
      }

      if (!serverEndpoints.keySet().containsAll(cacheMembers)) {
         if (log.isTraceEnabled()) log.trace("Postpone topology update");
         return Optional.empty(); // Postpone topology update
      }

      if (log.isTraceEnabled()) log.tracef("Send client topology update with topology id %s", clientTopologyId);

      if (clientIntel == Constants.INTELLIGENCE_HASH_DISTRIBUTION_AWARE && !cacheMode.isInvalidation()) {
         int numSegments = cacheTopology.getReadConsistentHash().getNumSegments();
         return Optional.of(new HashDistAware20Response(clientTopologyId, serverEndpoints, numSegments,
               Constants.DEFAULT_CONSISTENT_HASH_VERSION));
      } else {
         return Optional.of(new TopologyAwareResponse(clientTopologyId, serverEndpoints, 0));
      }
   }

   private static Optional<Integer> projectionInfo(List<CacheEntry> entries, byte version) {
      if (!entries.isEmpty()) {
         CacheEntry entry = entries.get(0);
         if (entry.getValue() instanceof Object[]) {
            return Optional.of(((Object[]) entry.getValue()).length);
         } else if (HotRodVersion.HOTROD_24.isAtLeast(version)) {
            return Optional.of(1);
         }
      }
      return Optional.empty();
   }
}
