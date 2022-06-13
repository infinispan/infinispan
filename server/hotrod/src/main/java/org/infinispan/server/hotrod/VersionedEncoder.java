package org.infinispan.server.hotrod;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.infinispan.CacheSet;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.hotrod.Events.Event;
import org.infinispan.server.hotrod.counter.listener.ClientCounterEvent;
import org.infinispan.server.hotrod.iteration.IterableIterationResult;
import org.infinispan.stats.ClusterCacheStats;
import org.infinispan.stats.Stats;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * This class represents the work to be done by an encoder of a particular Hot Rod protocol version.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public interface VersionedEncoder {

   ByteBuf authResponse(HotRodHeader header, HotRodServer server, Channel channel, byte[] challenge);

   ByteBuf authMechListResponse(HotRodHeader header, HotRodServer server, Channel channel, Set<String> mechs);

   ByteBuf notExecutedResponse(HotRodHeader header, HotRodServer server, Channel channel, CacheEntry<byte[], byte[]> prev);

   ByteBuf notExistResponse(HotRodHeader header, HotRodServer server, Channel channel);

   ByteBuf valueResponse(HotRodHeader header, HotRodServer server, Channel channel, OperationStatus status, byte[] prev);

   ByteBuf valueResponse(HotRodHeader header, HotRodServer server, Channel channel, OperationStatus status, CacheEntry<byte[], byte[]> prev);

   ByteBuf successResponse(HotRodHeader header, HotRodServer server, Channel channel, CacheEntry<byte[], byte[]> result);

   ByteBuf errorResponse(HotRodHeader header, HotRodServer server, Channel channel, String message, OperationStatus status);

   ByteBuf bulkGetResponse(HotRodHeader header, HotRodServer server, Channel channel, int size, CacheSet<Map.Entry<byte[], byte[]>> entries);

   ByteBuf emptyResponse(HotRodHeader header, HotRodServer server, Channel channel, OperationStatus status);

   default ByteBuf pingResponse(HotRodHeader header, HotRodServer server, Channel channel, OperationStatus status) {
      return emptyResponse(header, server, channel, status);
   }

   ByteBuf statsResponse(HotRodHeader header, HotRodServer server, Channel channel, Stats stats,
                         NettyTransport transport, ClusterCacheStats clusterCacheStats1);

   ByteBuf valueWithVersionResponse(HotRodHeader header, HotRodServer server, Channel channel, byte[] value, long version);

   ByteBuf getWithMetadataResponse(HotRodHeader header, HotRodServer server, Channel channel, CacheEntry<byte[], byte[]> entry);

   ByteBuf getStreamResponse(HotRodHeader header, HotRodServer server, Channel channel, int offset, CacheEntry<byte[], byte[]> entry);

   ByteBuf getAllResponse(HotRodHeader header, HotRodServer server, Channel channel, Map<byte[], byte[]> map);

   ByteBuf bulkGetKeysResponse(HotRodHeader header, HotRodServer server, Channel channel, CloseableIterator<byte[]> iterator);

   ByteBuf iterationStartResponse(HotRodHeader header, HotRodServer server, Channel channel, String iterationId);

   ByteBuf iterationNextResponse(HotRodHeader header, HotRodServer server, Channel channel, IterableIterationResult iterationResult);

   ByteBuf counterConfigurationResponse(HotRodHeader header, HotRodServer server, Channel channel, CounterConfiguration configuration);

   ByteBuf counterNamesResponse(HotRodHeader header, HotRodServer server, Channel channel, Collection<String> counterNames);

   ByteBuf multimapCollectionResponse(HotRodHeader header, HotRodServer server, Channel channel, OperationStatus status, Collection<byte[]> values);

   ByteBuf multimapEntryResponse(HotRodHeader header, HotRodServer server, Channel channel, OperationStatus status, CacheEntry<byte[], Collection<byte[]>> ce);

   ByteBuf booleanResponse(HotRodHeader header, HotRodServer server, Channel channel, boolean result);

   ByteBuf unsignedLongResponse(HotRodHeader header, HotRodServer server, Channel channel, long value);

   ByteBuf longResponse(HotRodHeader header, HotRodServer server, Channel channel, long value);

   ByteBuf transactionResponse(HotRodHeader header, HotRodServer server, Channel channel, int xaReturnCode);

   OperationStatus errorStatus(Throwable t);

   /**
    * Write an event, including its header, using the given channel buffer
    */
   void writeEvent(Event e, ByteBuf buf);

   /**
    * Writes a {@link ClientCounterEvent}, including its header, using a giver channel buffer.
    */
   void writeCounterEvent(ClientCounterEvent event, ByteBuf buffer);

   ByteBuf recoveryResponse(HotRodHeader header, HotRodServer server, Channel channel, Collection<XidImpl> xids);
}
