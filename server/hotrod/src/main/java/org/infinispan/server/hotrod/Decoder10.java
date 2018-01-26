package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readMaybeByte;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readMaybeLong;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readMaybeRangedBytes;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readMaybeString;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readMaybeVInt;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.Flag;
import org.infinispan.server.core.ServerConstants;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.stats.Stats;
import org.infinispan.util.concurrent.TimeoutException;

import io.netty.buffer.ByteBuf;

/**
 * HotRod protocol decoder specific for specification version 1.0.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public class Decoder10 implements VersionedDecoder {
   private final static Log log = LogFactory.getLog(Decoder10.class, Log.class);
   private final static boolean isTrace = log.isTraceEnabled();

   @Override
   public boolean readHeader(ByteBuf buffer, byte version, long messageId, HotRodHeader header) throws Exception {
      if (header.op == null) {
         Optional<Byte> maybeByte = readMaybeByte(buffer);
         if (!maybeByte.flatMap(streamOp -> readMaybeString(buffer).map(cacheName -> {
            header.op = HotRodOperation.fromRequestOpCode(streamOp);
            if (isTrace) log.tracef("Operation code: %d has been matched to %s", streamOp, header.op);
            header.cacheName = cacheName;
            buffer.markReaderIndex();
            return streamOp;
         })).isPresent()) {
            return false;
         } else if (header.op == null) {
            throw new HotRodUnknownOperationException("Unknown operation: " + maybeByte.get(), version, messageId);
         }
      }

      return readMaybeVInt(buffer).flatMap(flag -> readMaybeByte(buffer).flatMap(clientIntelligence ->
            readMaybeVInt(buffer).flatMap(topologyId -> readMaybeByte(buffer).map(txId -> {
               if (txId != 0) {
                  throw new UnsupportedOperationException("Transaction types other than 0 (NO_TX) is not supported " +
                        "at this stage.  Saw TX_ID of " + txId);
               }
               header.flag = flag;
               header.clientIntel = clientIntelligence;
               header.topologyId = topologyId;
               buffer.markReaderIndex();
               return txId;
            })))).isPresent();
   }

   @Override
   public CacheDecodeContext.RequestParameters readParameters(HotRodHeader header, ByteBuf buffer) {
      switch (header.op) {
         case REMOVE_IF_UNMODIFIED:
            return readMaybeLong(buffer).map(l -> new CacheDecodeContext.RequestParameters(-1,
                  new CacheDecodeContext.ExpirationParam(-1, TimeUnitValue.SECONDS),
                  new CacheDecodeContext.ExpirationParam(-1, TimeUnitValue.SECONDS), l)
            ).orElse(null);
         case REPLACE_IF_UNMODIFIED:
            return readLifespanOrMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultLifespan)).flatMap(lifespan ->
                  readLifespanOrMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultMaxIdle)).flatMap(maxIdle ->
                        readMaybeLong(buffer).flatMap(version -> readMaybeVInt(buffer).map(valueLength ->
                              new CacheDecodeContext.RequestParameters(valueLength,
                                    new CacheDecodeContext.ExpirationParam(lifespan, TimeUnitValue.SECONDS),
                                    new CacheDecodeContext.ExpirationParam(maxIdle, TimeUnitValue.SECONDS), version)
                        ))
                  )
            ).orElse(null);
         default:
            return readLifespanOrMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultLifespan)).flatMap(lifespan ->
                  readLifespanOrMaxIdle(buffer, hasFlag(header, ProtocolFlag.DefaultMaxIdle)).flatMap(maxIdle ->
                        readMaybeVInt(buffer).map(valueLength ->
                              new CacheDecodeContext.RequestParameters(valueLength,
                                    new CacheDecodeContext.ExpirationParam(lifespan, TimeUnitValue.SECONDS),
                                    new CacheDecodeContext.ExpirationParam(maxIdle, TimeUnitValue.SECONDS), -1)
                        )
                  )
            ).orElse(null);
      }
   }

   private boolean hasFlag(HotRodHeader h, ProtocolFlag f) {
      return (h.flag & f.getValue()) == f.getValue();
   }

   private Optional<Integer> readLifespanOrMaxIdle(ByteBuf buffer, boolean useDefault) {
      return readMaybeVInt(buffer).map(stream -> {
         if (stream <= 0) {
            if (useDefault)
               return ServerConstants.EXPIRATION_DEFAULT;
            else
               return ServerConstants.EXPIRATION_NONE;
         } else return stream;
      });
   }

   @Override
   public Response createSuccessResponse(HotRodHeader header, byte[] prev) {
      return createResponse(header, OperationStatus.Success, prev);
   }

   @Override
   public Response createNotExecutedResponse(HotRodHeader header, byte[] prev) {
      return createResponse(header, OperationStatus.OperationNotExecuted, prev);
   }

   @Override
   public Response createNotExistResponse(HotRodHeader header) {
      return createResponse(header, OperationStatus.KeyDoesNotExist, null);
   }

   private Response createResponse(HotRodHeader h, OperationStatus st, byte[] prev) {
      if (hasFlag(h, ProtocolFlag.ForceReturnPreviousValue))
         return new ResponseWithPrevious(h.version, h.messageId, h.cacheName,
               h.clientIntel, h.op, st, h.topologyId, Optional.ofNullable(prev));
      else
         return new EmptyResponse(h.version, h.messageId, h.cacheName, h.clientIntel, h.op, st, h.topologyId);
   }

   @Override
   public Response createGetResponse(HotRodHeader h, CacheEntry<byte[], byte[]> entry) {
      HotRodOperation op = h.op;
      if (entry != null && op == HotRodOperation.GET)
         return new GetResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               op, OperationStatus.Success, h.topologyId,
               entry.getValue());
      else if (entry != null && op == HotRodOperation.GET_WITH_VERSION) {
         long version = ((NumericVersion) entry.getMetadata().version()).getVersion();
         return new GetWithVersionResponse(h.version, h.messageId, h.cacheName,
               h.clientIntel, op, OperationStatus.Success, h.topologyId,
               entry.getValue(), version);
      } else if (op == HotRodOperation.GET)
         return new GetResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               op, OperationStatus.KeyDoesNotExist, h.topologyId, null);
      else
         return new GetWithVersionResponse(h.version, h.messageId, h.cacheName,
               h.clientIntel, op, OperationStatus.KeyDoesNotExist,
               h.topologyId, null, 0);
   }

   @Override
   public void customReadHeader(HotRodHeader header, ByteBuf buffer, CacheDecodeContext hrCtx, List<Object> out) {
      // Do nothing
   }

   @Override
   public void customReadKey(HotRodHeader h, ByteBuf buffer, CacheDecodeContext hrCtx, List<Object> out) {
      switch (h.op) {
         case BULK_GET:
         case BULK_GET_KEYS:
            Optional<Integer> number = readMaybeVInt(buffer);
            number.ifPresent(n -> {
               hrCtx.operationDecodeContext = n;
               buffer.markReaderIndex();
               out.add(hrCtx);
            });
            break;
         case QUERY:
            Optional<byte[]> bytes = readMaybeRangedBytes(buffer);
            bytes.ifPresent(b -> {
               hrCtx.operationDecodeContext = b;
               buffer.markReaderIndex();
               out.add(hrCtx);
            });
            break;
      }
   }

   @Override
   public void customReadValue(HotRodHeader header, ByteBuf buffer, CacheDecodeContext hrCtx, List<Object> out) {
      // Do nothing
   }

   @Override
   public StatsResponse createStatsResponse(CacheDecodeContext hrCtx, Stats cacheStats, NettyTransport t) {
      HotRodHeader h = hrCtx.header;
      Map<String, String> stats = new HashMap<>();
      stats.put("timeSinceStart", String.valueOf(cacheStats.getTimeSinceStart()));
      stats.put("currentNumberOfEntries", String.valueOf(cacheStats.getCurrentNumberOfEntries()));
      stats.put("totalNumberOfEntries", String.valueOf(cacheStats.getTotalNumberOfEntries()));
      stats.put("stores", String.valueOf(cacheStats.getStores()));
      stats.put("retrievals", String.valueOf(cacheStats.getRetrievals()));
      stats.put("hits", String.valueOf(cacheStats.getHits()));
      stats.put("misses", String.valueOf(cacheStats.getMisses()));
      stats.put("removeHits", String.valueOf(cacheStats.getRemoveHits()));
      stats.put("removeMisses", String.valueOf(cacheStats.getRemoveMisses()));
      stats.put("totalBytesRead", t.getTotalBytesRead());
      stats.put("totalBytesWritten", t.getTotalBytesWritten());
      return new StatsResponse(h.version, h.messageId, h.cacheName, h.clientIntel, stats, h.topologyId);
   }

   @Override
   public ErrorResponse createErrorResponse(HotRodHeader header, Throwable t) {
      if (t instanceof IOException) {
         return new ErrorResponse(header.version, header.messageId, header.cacheName, header.clientIntel,
               OperationStatus.ParseError, header.topologyId, t.toString());
      } else if (t instanceof TimeoutException) {
         return new ErrorResponse(header.version, header.messageId, header.cacheName, header.clientIntel,
               OperationStatus.OperationTimedOut, header.topologyId, t.toString());
      } else {
         return new ErrorResponse(header.version, header.messageId, header.cacheName, header.clientIntel,
               OperationStatus.ServerError, header.topologyId, t.toString());
      }
   }

   @Override
   public AdvancedCache<byte[], byte[]> getOptimizedCache(HotRodHeader h, AdvancedCache<byte[], byte[]> c, Configuration cacheCfg) {
      if (!hasFlag(h, ProtocolFlag.ForceReturnPreviousValue)) {
         switch (h.op) {
            case PUT:
            case PUT_IF_ABSENT:
               return c.withFlags(Flag.IGNORE_RETURN_VALUES);
         }
      }
      return c;
   }

   @Override
   public boolean isSkipCacheLoad(HotRodHeader header) {
      return false;
   }

   @Override
   public boolean isSkipIndexing(HotRodHeader header) {
      return false;
   }
}
