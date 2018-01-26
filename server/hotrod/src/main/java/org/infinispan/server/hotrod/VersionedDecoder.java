package org.infinispan.server.hotrod;

import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.hotrod.CacheDecodeContext.RequestParameters;
import org.infinispan.stats.Stats;

import io.netty.buffer.ByteBuf;

/**
 * This class represents the work to be done by a decoder of a particular Hot Rod protocol version.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
interface VersionedDecoder {

   /**
    * Having read the message's Id, read the rest of Hot Rod header from the given buffer and return it. Returns whether
    * the entire header was read or not.
    */
   boolean readHeader(ByteBuf buffer, byte version, long messageId, HotRodHeader header) throws Exception;

   /**
    * Read the parameters of the operation, if present.
    */
   RequestParameters readParameters(HotRodHeader header, ByteBuf buffer);

   /**
    * Create a successful response.
    */
   Response createSuccessResponse(HotRodHeader header, byte[] prev);

   /**
    * Create a response indicating the the operation could not be executed.
    */
   Response createNotExecutedResponse(HotRodHeader header, byte[] prev);

   /**
    * Create a response indicating that the key, which the message tried to operate on, did not exist.
    */
   Response createNotExistResponse(HotRodHeader header);

   /**
    * Create a response for get a request.
    */
   Response createGetResponse(HotRodHeader header, CacheEntry<byte[], byte[]> entry);

   /**
    * Read operation specific data for an operation that only requires a header
    */
   void customReadHeader(HotRodHeader header, ByteBuf buffer, CacheDecodeContext hrCtx, List<Object> out);

   /**
    * Handle a protocol specific key reading.
    */
   void customReadKey(HotRodHeader header, ByteBuf buffer, CacheDecodeContext hrCtx, List<Object> out);

   /**
    * Handle a protocol specific value reading.
    */
   void customReadValue(HotRodHeader header, ByteBuf buffer, CacheDecodeContext hrCtx, List<Object> out);

   /**
    * Create a response for the stats command.
    */
   StatsResponse createStatsResponse(CacheDecodeContext ctx, Stats cacheStats, NettyTransport t);

   /**
    * Create an error response based on the Throwable instance received.
    */
   ErrorResponse createErrorResponse(HotRodHeader header, Throwable t);

   /**
    * Get an optimized cache instance depending on the operation parameters.
    */
   AdvancedCache<byte[], byte[]> getOptimizedCache(HotRodHeader h, AdvancedCache<byte[], byte[]> c, Configuration cacheCfg);

   boolean isSkipCacheLoad(HotRodHeader header);

   boolean isSkipIndexing(HotRodHeader header);
}
