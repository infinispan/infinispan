package org.infinispan.server.hotrod;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.transaction.xa.Xid;

import org.infinispan.CacheSet;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.hotrod.Events.Event;
import org.infinispan.server.hotrod.counter.listener.ClientCounterEvent;
import org.infinispan.server.hotrod.iteration.IterableIterationResult;
import org.infinispan.stats.Stats;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * This class represents the work to be done by an encoder of a particular Hot Rod protocol version.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public interface VersionedEncoder {

   ByteBuf authResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, byte[] challenge);

   ByteBuf authMechListResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, Set<String> mechs);

   ByteBuf notExecutedResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, byte[] prev);

   ByteBuf notExistResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc);

   ByteBuf valueResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, OperationStatus status, byte[] prev);

   ByteBuf successResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, byte[] result);

   ByteBuf errorResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, String message, OperationStatus status);

   ByteBuf bulkGetResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, int size, CacheSet<Map.Entry<byte[], byte[]>> entries);

   ByteBuf emptyResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, OperationStatus status);

   default ByteBuf pingResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, OperationStatus status) {
      return emptyResponse(header, server, alloc, status);
   }

   ByteBuf statsResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, Stats stats, NettyTransport transport, ComponentRegistry cacheRegistry);

   ByteBuf valueWithVersionResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, byte[] value, long version);

   ByteBuf getWithMetadataResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, CacheEntry<byte[], byte[]> entry);

   ByteBuf getStreamResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, int offset, CacheEntry<byte[], byte[]> entry);

   ByteBuf getAllResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, Map<byte[], byte[]> map);

   ByteBuf bulkGetKeysResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, CloseableIterator<byte[]> iterator);

   ByteBuf iterationStartResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, String iterationId);

   ByteBuf iterationNextResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, IterableIterationResult iterationResult);

   ByteBuf counterConfigurationResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, CounterConfiguration configuration);

   ByteBuf counterNamesResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, Collection<String> counterNames);

   ByteBuf multimapCollectionResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, OperationStatus status, Collection<byte[]> values);

   ByteBuf multimapEntryResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, OperationStatus status, CacheEntry<WrappedByteArray, Collection<WrappedByteArray>> ce, Collection<byte[]> result);

   ByteBuf booleanResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, boolean result);

   ByteBuf unsignedLongResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, long value);

   ByteBuf longResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, long value);

   ByteBuf transactionResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, int xaReturnCode);

   OperationStatus errorStatus(Throwable t);

   /**
    * Write an event, including its header, using the given channel buffer
    */
   void writeEvent(Event e, ByteBuf buf);

   /**
    * Writes a {@link ClientCounterEvent}, including its header, using a giver channel buffer.
    */
   void writeCounterEvent(ClientCounterEvent event, ByteBuf buffer);

   ByteBuf recoveryResponse(HotRodHeader header, HotRodServer server, ByteBufAllocator alloc, Collection<Xid> xids);
}
