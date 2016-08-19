package org.infinispan.server.hotrod;

import java.io.IOException;
import java.security.PrivilegedActionException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.Flag;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.server.core.transport.ExtendedByteBufJava;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.transport.ExtendedByteBuf;
import org.infinispan.stats.ClusterCacheStats;
import org.infinispan.stats.Stats;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.concurrent.TimeoutException;
import org.jgroups.SuspectedException;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

/**
 * HotRod protocol decoder specific for specification version 2.0.
 *
 * @author Galder Zamarreño
 * @since 7.0
 */
class Decoder2x implements VersionedDecoder {

   private static final Log log = LogFactory.getLog(Decoder2x.class, Log.class);

   private static final long EXPIRATION_NONE = -1;
   private static final long EXPIRATION_DEFAULT = -2;

   private static final CacheDecodeContext.ExpirationParam DEFAULT_EXPIRATION =
         new CacheDecodeContext.ExpirationParam(-1, TimeUnitValue.SECONDS);

   @Override
   public boolean readHeader(ByteBuf buffer, byte version, long messageId, HotRodHeader header) throws Exception {
      if (header.op == null) {
         int readableBytes = buffer.readableBytes();
         // We require at least 2 bytes at minimum
         if (readableBytes < 2) {
            return false;
         }
         byte streamOp = buffer.readByte();
         int length = ExtendedByteBufJava.readMaybeVInt(buffer);
         // Didn't have enough bytes for VInt or the length is too long for remaining
         if (length == Integer.MIN_VALUE || length > buffer.readableBytes()) {
            return false;
         } else if (length == 0) {
            header.cacheName = "";
         } else {
            byte[] bytes = new byte[length];
            buffer.readBytes(bytes);
            header.cacheName = new String(bytes, CharsetUtil.UTF_8);
         }
         switch (streamOp) {
            case 0x01:
               header.op = HotRodOperation.PutRequest;
               break;
            case 0x03:
               header.op = HotRodOperation.GetRequest;
               break;
            case 0x05:
               header.op = HotRodOperation.PutIfAbsentRequest;
               break;
            case 0x07:
               header.op = HotRodOperation.ReplaceRequest;
               break;
            case 0x09:
               header.op = HotRodOperation.ReplaceIfUnmodifiedRequest;
               break;
            case 0x0B:
               header.op = HotRodOperation.RemoveRequest;
               break;
            case 0x0D:
               header.op = HotRodOperation.RemoveIfUnmodifiedRequest;
               break;
            case 0x0F:
               header.op = HotRodOperation.ContainsKeyRequest;
               break;
            case 0x11:
               header.op = HotRodOperation.GetWithVersionRequest;
               break;
            case 0x13:
               header.op = HotRodOperation.ClearRequest;
               break;
            case 0x15:
               header.op = HotRodOperation.StatsRequest;
               break;
            case 0x17:
               header.op = HotRodOperation.PingRequest;
               break;
            case 0x19:
               header.op = HotRodOperation.BulkGetRequest;
               break;
            case 0x1B:
               header.op = HotRodOperation.GetWithMetadataRequest;
               break;
            case 0x1D:
               header.op = HotRodOperation.BulkGetKeysRequest;
               break;
            case 0x1F:
               header.op = HotRodOperation.QueryRequest;
               break;
            case 0x21:
               header.op = HotRodOperation.AuthMechListRequest;
               break;
            case 0x23:
               header.op = HotRodOperation.AuthRequest;
               break;
            case 0x25:
               header.op = HotRodOperation.AddClientListenerRequest;
               break;
            case 0x27:
               header.op = HotRodOperation.RemoveClientListenerRequest;
               break;
            case 0x29:
               header.op = HotRodOperation.SizeRequest;
               break;
            case 0x2B:
               header.op = HotRodOperation.ExecRequest;
               break;
            case 0x2D:
               header.op = HotRodOperation.PutAllRequest;
               break;
            case 0x2F:
               header.op = HotRodOperation.GetAllRequest;
               break;
            case 0x31:
               header.op = HotRodOperation.IterationStartRequest;
               break;
            case 0x33:
               header.op = HotRodOperation.IterationNextRequest;
               break;
            case 0x35:
               header.op = HotRodOperation.IterationEndRequest;
               break;
            default:
               throw new HotRodUnknownOperationException(
                     "Unknown operation: " + streamOp, version, messageId);
         }
         buffer.markReaderIndex();
      }
      int flag = ExtendedByteBufJava.readMaybeVInt(buffer);
      if (flag == Integer.MIN_VALUE) {
         return false;
      }
      if (buffer.readableBytes() < 2) {
         return false;
      }
      byte clientIntelligence = buffer.readByte();
      int topologyId = ExtendedByteBufJava.readMaybeVInt(buffer);
      if (topologyId == Integer.MIN_VALUE) {
         return false;
      }
      header.flag = flag;
      header.clientIntel = clientIntelligence;
      header.topologyId = topologyId;

      buffer.markReaderIndex();
      return true;
   }

   @Override
   public CacheDecodeContext.RequestParameters readParameters(HotRodHeader header, ByteBuf buffer) {
      switch (header.op) {
         case RemoveIfUnmodifiedRequest:
            return readParameters(buffer, header, false, false, true);
         case ReplaceIfUnmodifiedRequest:
            return readParameters(buffer, header, true, true, true);
         case GetAllRequest:
            return readParameters(buffer, header, false, true, false);
         default:
            return readParameters(buffer, header, true, true, false);
      }
   }

   private static CacheDecodeContext.RequestParameters readParameters(ByteBuf buffer, HotRodHeader header, boolean readExpiration,
                                                                      boolean readSize, boolean readVersion) {
      CacheDecodeContext.ExpirationParam param1;
      CacheDecodeContext.ExpirationParam param2;
      long version;
      int size;

      if (readExpiration) {
         boolean pre22Version = Constants.isVersionPre22(header.version);
         byte firstUnit;
         byte secondUnit;
         if (pre22Version) {
            firstUnit = secondUnit = TimeUnitValue.SECONDS.getCode();
         } else {
            if (buffer.readableBytes() == 0) {
               return null;
            }
            byte units = buffer.readByte();
            firstUnit = (byte) ((units & 0xf0) >> 4);
            secondUnit = (byte) (units & 0x0f);
         }
         param1 = readExpirationParam(pre22Version, hasFlag(header, ProtocolFlag.DefaultLifespan), buffer, firstUnit);
         if (param1 == null) {
            return null;
         }
         param2 = readExpirationParam(pre22Version, hasFlag(header, ProtocolFlag.DefaultMaxIdle), buffer, secondUnit);
         if (param2 == null) {
            return null;
         }
      } else {
         param1 = param2 = DEFAULT_EXPIRATION;
      }
      if (readVersion) {
         version = ExtendedByteBufJava.readUnsignedMaybeLong(buffer);
         if (version == Integer.MIN_VALUE) {
            return null;
         }
      } else {
         version = -1;
      }
      if (readSize) {
         size = ExtendedByteBufJava.readMaybeVInt(buffer);
         if (size == Integer.MIN_VALUE) {
            return null;
         }
      } else {
         size = -1;
      }
      buffer.markReaderIndex();
      return new CacheDecodeContext.RequestParameters(size, param1, param2, version);
   }

   private static CacheDecodeContext.ExpirationParam readExpirationParam(boolean pre22Version, boolean useDefault, ByteBuf buffer,
                                                                         byte timeUnit) {
      if (pre22Version) {
         int duration = ExtendedByteBufJava.readMaybeVInt(buffer);
         if (duration == Integer.MIN_VALUE) {
            return null;
         } else if (duration <= 0) {
            duration = useDefault ? (int) EXPIRATION_DEFAULT : (int) EXPIRATION_NONE;
         }
         return new CacheDecodeContext.ExpirationParam(duration, TimeUnitValue.decode(timeUnit));
      } else {
         switch (timeUnit) {
            // Default time unit
            case 0x07:
               return new CacheDecodeContext.ExpirationParam(EXPIRATION_DEFAULT, TimeUnitValue.decode(timeUnit));
            // Infinite time unit
            case 0x08:
               return new CacheDecodeContext.ExpirationParam(EXPIRATION_NONE, TimeUnitValue.decode(timeUnit));
            default:
               long timeDuration = ExtendedByteBufJava.readMaybeVLong(buffer);
               if (timeDuration == Long.MIN_VALUE) {
                  return null;
               }
               return new CacheDecodeContext.ExpirationParam(timeDuration, TimeUnitValue.decode(timeUnit));
         }
      }
   }

   private static boolean hasFlag(HotRodHeader h, ProtocolFlag f) {
      return (h.flag & f.getValue()) == f.getValue();
   }

   @Override
   public Response createSuccessResponse(HotRodHeader header, byte[] prev) {
      return createResponse(header, OperationResponse.toResponse(header.op), OperationStatus.Success, prev);
   }

   @Override
   public Response createNotExecutedResponse(HotRodHeader header, byte[] prev) {
      return createResponse(header, OperationResponse.toResponse(header.op), OperationStatus.OperationNotExecuted, prev);
   }

   @Override
   public Response createNotExistResponse(HotRodHeader header) {
      return createResponse(header, OperationResponse.toResponse(header.op), OperationStatus.KeyDoesNotExist, null);
   }

   private Response createResponse(HotRodHeader h, OperationResponse op, OperationStatus st, byte[] prev) {
      if (hasFlag(h, ProtocolFlag.ForceReturnPreviousValue)) {
         switch (st) {
            case Success:
               switch (h.op) {
                  case PutRequest:
                  case ReplaceRequest:
                  case RemoveIfUnmodifiedRequest:
                  case RemoveRequest:
                  case ReplaceIfUnmodifiedRequest:
                     return new ResponseWithPrevious(h.version, h.messageId, h.cacheName,
                           h.clientIntel, op, OperationStatus.SuccessWithPrevious, h.topologyId, Optional.ofNullable(prev));
               }
               break;
            case OperationNotExecuted:
               switch (h.op) {
                  case PutIfAbsentRequest:
                  case ReplaceIfUnmodifiedRequest:
                  case RemoveIfUnmodifiedRequest:
                     return new ResponseWithPrevious(h.version, h.messageId, h.cacheName,
                           h.clientIntel, op, OperationStatus.NotExecutedWithPrevious, h.topologyId, Optional.ofNullable(prev));
               }
               break;
         }
      }
      return new Response(h.version, h.messageId, h.cacheName, h.clientIntel, op, st, h.topologyId);
   }

   @Override
   public Response createGetResponse(HotRodHeader h, CacheEntry<byte[], byte[]> entry) {
      HotRodOperation op = h.op;
      if (entry != null && op == HotRodOperation.GetRequest)
         return new GetResponse(h.version, h.messageId, h.cacheName, h.clientIntel, OperationResponse.GetResponse,
               OperationStatus.Success, h.topologyId, entry.getValue());
      else if (entry != null && op == HotRodOperation.GetWithVersionRequest) {
         long version;
         NumericVersion numericVersion = (NumericVersion) entry.getMetadata().version();
         if (numericVersion != null) {
            version = numericVersion.getVersion();
         } else {
            version = 0;
         }
         return new GetWithVersionResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               OperationResponse.GetWithVersionResponse, OperationStatus.Success, h.topologyId, entry.getValue(),
               version);
      } else if (op == HotRodOperation.GetRequest)
         return new GetResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               OperationResponse.GetResponse, OperationStatus.KeyDoesNotExist, h.topologyId, null);
      else
         return new GetWithVersionResponse(h.version, h.messageId, h.cacheName,
               h.clientIntel, OperationResponse.GetWithVersionResponse, OperationStatus.KeyDoesNotExist,
               h.topologyId, null, 0);
   }

   @Override
   public void customReadHeader(HotRodHeader header, ByteBuf buffer, CacheDecodeContext hrCtx, List<Object> out) {
      switch (header.op) {
         case AuthRequest:
            ExtendedByteBuf.readMaybeString(buffer).flatMap(mech ->
                  ExtendedByteBuf.readMaybeRangedBytes(buffer).map(clientResponse -> {
                     hrCtx.operationDecodeContext = new KeyValuePair<>(mech, clientResponse);
                     buffer.markReaderIndex();
                     out.add(hrCtx);
                     return null;
                  }));
            break;
         case ExecRequest:
            ExecRequestContext execCtx = (ExecRequestContext) hrCtx.operationDecodeContext;
            // first time read
            if (execCtx == null) {
               Optional<ExecRequestContext> optional = ExtendedByteBuf.readMaybeString(buffer).flatMap(name ->
                     ExtendedByteBuf.readMaybeVInt(buffer).map(paramCount -> {
                        ExecRequestContext ctx = new ExecRequestContext(name, paramCount, new HashMap<>(paramCount));
                        hrCtx.operationDecodeContext = ctx;
                        // Mark that we read these
                        buffer.markReaderIndex();

                        return ctx;
                     }));
               if (optional.isPresent()) {
                  execCtx = optional.get();
               } else {
                  return;
               }
            }

            if (execCtx.getParamSize() == 0) {
               out.add(hrCtx);
            } else {
               Map<String, byte[]> map = execCtx.getParams();
               boolean readAll = true;
               while (map.size() < execCtx.getParamSize()) {
                  if (!ExtendedByteBuf.readMaybeString(buffer).flatMap(key ->
                        ExtendedByteBuf.readMaybeRangedBytes(buffer).map(value -> {
                           map.put(key, value);
                           buffer.markReaderIndex();
                           return value;
                        })).isPresent()) {
                     readAll = false;
                     break;
                  }
               }
               if (readAll) {
                  out.add(hrCtx);
               }
            }
            break;
         default:
            // This operation doesn't need additional reads - has everything to process
            out.add(hrCtx);
      }
   }

   @Override
   public void customReadKey(HotRodHeader header, ByteBuf buffer, CacheDecodeContext hrCtx, List<Object> out) {
      switch (header.op) {
         case BulkGetRequest:
         case BulkGetKeysRequest:
            ExtendedByteBuf.readMaybeVInt(buffer).ifPresent(number -> {
               hrCtx.operationDecodeContext = number;
               buffer.markReaderIndex();
               out.add(hrCtx);
            });
            break;
         case QueryRequest:
            ExtendedByteBuf.readMaybeRangedBytes(buffer).ifPresent(query -> {
               hrCtx.operationDecodeContext = query;
               buffer.markReaderIndex();
               out.add(hrCtx);
            });
            break;
         case AddClientListenerRequest:
            ClientListenerRequestContext requestCtx;
            if (hrCtx.operationDecodeContext == null) {
               Optional<ClientListenerRequestContext> optional = ExtendedByteBuf.readMaybeRangedBytes(buffer).flatMap(listenerId ->
                     ExtendedByteBuf.readMaybeByte(buffer).map(includeState -> {
                        ClientListenerRequestContext ctx = new ClientListenerRequestContext(listenerId, includeState == 1);
                        hrCtx.operationDecodeContext = ctx;
                        // Mark that we read these
                        buffer.markReaderIndex();

                        return ctx;
                     }));
               if (optional.isPresent()) {
                  requestCtx = optional.get();
               } else {
                  return;
               }
            } else {
               requestCtx = (ClientListenerRequestContext) hrCtx.operationDecodeContext;
            }
            if (requestCtx.getFilterFactoryInfo() == null) {
               if (!readMaybeNamedFactory(buffer).map(f -> {
                  requestCtx.setFilterFactoryInfo(f);
                  buffer.markReaderIndex();
                  return requestCtx;
               }).isPresent()) {
                  return;
               }
            }
            if (!readMaybeNamedFactory(buffer).map(converter -> {
               boolean useRawData;
               if (Constants.isVersion2x(header.version)) {
                  Optional<Byte> rawOptional = ExtendedByteBuf.readMaybeByte(buffer);
                  if (rawOptional.isPresent()) {
                     useRawData = rawOptional.get() == 1;
                  } else {
                     return null;
                  }
               } else {
                  useRawData = false;
               }
               requestCtx.setConverterFactoryInfo(converter);
               requestCtx.setUseRawData(useRawData);

               buffer.markReaderIndex();
               out.add(hrCtx);

               return requestCtx;
            }).isPresent()) {
               return;
            }
            break;
         case RemoveClientListenerRequest:
            ExtendedByteBuf.readMaybeRangedBytes(buffer).ifPresent(listenerId -> {
               hrCtx.operationDecodeContext = listenerId;
               buffer.markReaderIndex();
               out.add(hrCtx);
            });
            break;
         case IterationStartRequest:
            ExtendedByteBuf.readMaybeOptRangedBytes(buffer).flatMap(segments ->
                  ExtendedByteBuf.readMaybeOptString(buffer).map(name -> {
                     Optional<KeyValuePair<String, List<byte[]>>> factory;
                     boolean isPre24 = Constants.isVersionPre24(header.version);
                     if (name.isPresent()) {
                        if (isPre24) {
                           factory = Optional.of(new KeyValuePair<>(name.get(), Collections.emptyList()));
                        } else {
                           Optional<List<byte[]>> optionalParams = readOptionalParams(buffer);
                           if (optionalParams.isPresent()) {
                              factory = Optional.of(new KeyValuePair<>(name.get(), optionalParams.get()));
                           } else {
                              return null;
                           }
                        }
                     } else {
                        factory = Optional.empty();
                     }
                     int batchSize;
                     Optional<Integer> optionalBatchSize = ExtendedByteBuf.readMaybeVInt(buffer);
                     if (optionalBatchSize.isPresent()) {
                        batchSize = optionalBatchSize.get();
                     } else {
                        return null;
                     }
                     boolean metadata;
                     if (isPre24) {
                        metadata = false;
                     } else {
                        Optional<Byte> optionalMetadata = ExtendedByteBuf.readMaybeByte(buffer);
                        if (optionalMetadata.isPresent()) {
                           metadata = optionalMetadata.get() != 0;
                        } else {
                           return null;
                        }
                     }
                     hrCtx.operationDecodeContext = new IterationStartRequest(segments, factory, batchSize, metadata);
                     buffer.markReaderIndex();
                     out.add(hrCtx);
                     return null;
                  }));
         case IterationNextRequest:
         case IterationEndRequest:
            ExtendedByteBuf.readMaybeString(buffer).ifPresent(iterationId -> {
               hrCtx.operationDecodeContext = iterationId;
               buffer.markReaderIndex();
               out.add(hrCtx);
            });
            break;
      }
   }

   private Optional<Optional<KeyValuePair<String, List<byte[]>>>> readMaybeNamedFactory(ByteBuf buffer) {
      return ExtendedByteBuf.readMaybeString(buffer).flatMap(name -> {
         if (!name.isEmpty()) {
            return readOptionalParams(buffer).map(param -> Optional.of(new KeyValuePair<>(name, param)));
         } else return Optional.of(Optional.empty());
      });
   }

   private Optional<List<byte[]>> readOptionalParams(ByteBuf buffer) {
      Optional<Byte> numParams = ExtendedByteBuf.readMaybeByte(buffer);
      return numParams.map(p -> {
         if (p > 0) {
            List<byte[]> params = new ArrayList<>();
            boolean readAll = true;
            while (params.size() < p) {
               if (!ExtendedByteBuf.readMaybeRangedBytes(buffer).map(param -> {
                  params.add(param);
                  buffer.markReaderIndex();
                  return param;
               }).isPresent()) {
                  readAll = false;
                  break;
               }
            }
            if (readAll) {
               return Optional.of(params);
            }
            return null;
         } else return Optional.<List<byte[]>>of(Collections.emptyList());
      }).orElse(Optional.empty());
   }

   GetWithMetadataResponse getKeyMetadata(HotRodHeader h, byte[] k, AdvancedCache<byte[], byte[]> cache) {
      CacheEntry<byte[], byte[]> ce = cache.getCacheEntry(k);
      if (ce != null) {
         NumericVersion entryVersion = (NumericVersion) ce.getMetadata().version();
         byte[] v = ce.getValue();
         int lifespan = ce.getLifespan() < 0 ? -1 : (int) (ce.getLifespan() / 1000);
         int maxIdle = ce.getMaxIdle() < 0 ? -1 : (int) (ce.getMaxIdle() / 1000);
         return new GetWithMetadataResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               OperationResponse.GetWithMetadataResponse, OperationStatus.Success, h.topologyId, v,
               entryVersion.getVersion(), ce.getCreated(), lifespan, ce.getLastUsed(), maxIdle);
      } else {
         return new GetWithMetadataResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               OperationResponse.GetWithMetadataResponse, OperationStatus.KeyDoesNotExist, h.topologyId, null,
               0, -1, -1, -1, -1);
      }
   }

   @Override
   public void customReadValue(HotRodHeader header, ByteBuf buffer, CacheDecodeContext hrCtx, List<Object> out) {
      switch (header.op) {
         case PutAllRequest:
            int maxLength = hrCtx.params.valueLength;
            Map<byte[], byte[]> map;
            if (hrCtx.operationDecodeContext == null) {
               map = new HashMap<>(maxLength);
               hrCtx.operationDecodeContext = map;
            } else {
               map = (Map<byte[], byte[]>) hrCtx.operationDecodeContext;
            }
            boolean readAll = true;
            while (map.size() < maxLength) {
               if (!ExtendedByteBuf.readMaybeRangedBytes(buffer).flatMap(key ->
                     ExtendedByteBuf.readMaybeRangedBytes(buffer).map(value -> {
                        map.put(key, value);
                        buffer.markReaderIndex();
                        return value;
                     })).isPresent()) {
                  readAll = false;
                  break;
               }
            }
            if (readAll) {
               out.add(hrCtx);
            }
            break;
         case GetAllRequest:
            maxLength = hrCtx.params.valueLength;
            Set<byte[]> set;
            if (hrCtx.operationDecodeContext == null) {
               set = new HashSet<>(maxLength);
               hrCtx.operationDecodeContext = set;
            } else {
               set = (Set<byte[]>) hrCtx.operationDecodeContext;
            }
            readAll = true;
            while (set.size() < maxLength) {
               if (!ExtendedByteBuf.readMaybeRangedBytes(buffer).map(bytes -> {
                  set.add(bytes);
                  buffer.markReaderIndex();
                  return bytes;
               }).isPresent()) {
                  readAll = false;
                  break;
               }
            }
            if (readAll) {
               out.add(hrCtx);
            }
            break;
      }
   }

   @Override
   public StatsResponse createStatsResponse(CacheDecodeContext ctx, NettyTransport t) {
      Stats cacheStats = ctx.cache.getStats();
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

      HotRodHeader h = ctx.header;
      if (!Constants.isVersionPre24(h.version)) {
         ComponentRegistry registry = ctx.getCacheRegistry(h.cacheName);
         ClusterCacheStats clusterCacheStats = registry.getComponent(ClusterCacheStats.class);
         if (clusterCacheStats != null) {
            stats.put("globalCurrentNumberOfEntries", String.valueOf(clusterCacheStats.getCurrentNumberOfEntries()));
            stats.put("globalStores", String.valueOf(clusterCacheStats.getStores()));
            stats.put("globalRetrievals", String.valueOf(clusterCacheStats.getRetrievals()));
            stats.put("globalHits", String.valueOf(clusterCacheStats.getHits()));
            stats.put("globalMisses", String.valueOf(clusterCacheStats.getMisses()));
            stats.put("globalRemoveHits", String.valueOf(clusterCacheStats.getRemoveHits()));
            stats.put("globalRemoveMisses", String.valueOf(clusterCacheStats.getRemoveMisses()));
         }
      }
      return new StatsResponse(h.version, h.messageId, h.cacheName, h.clientIntel, stats, h.topologyId);
   }

   @Override
   public ErrorResponse createErrorResponse(HotRodHeader h, Throwable t) {
      if (t instanceof SuspectException) {
         return createNodeSuspectedErrorResponse(h, t);
      } else if (t instanceof IllegalLifecycleStateException) {
         return createIllegalLifecycleStateErrorResponse(h, t);
      } else if (t instanceof IOException) {
         return new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel, OperationStatus.ParseError,
               h.topologyId, t.toString());
      } else if (t instanceof TimeoutException) {
         return new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               OperationStatus.OperationTimedOut, h.topologyId, t.toString());
      } else if (t instanceof CacheException) {
         // JGroups and remote exceptions (inside RemoteException) can come wrapped up
         Throwable cause = t.getCause();
         if (cause instanceof SuspectedException) {
            return createNodeSuspectedErrorResponse(h, cause);
         } else if (cause instanceof IllegalLifecycleStateException) {
            return createIllegalLifecycleStateErrorResponse(h, cause);
         } else if (cause instanceof InterruptedException) {
            return createIllegalLifecycleStateErrorResponse(h, cause);
         } else {
            return createServerErrorResponse(h, cause);
         }
      } else if (t instanceof InterruptedException) {
         return createIllegalLifecycleStateErrorResponse(h, t);
      } else if (t instanceof PrivilegedActionException) {
         return createErrorResponse(h, t.getCause());
      } else {
         return createServerErrorResponse(h, t);
      }
   }

   private ErrorResponse createNodeSuspectedErrorResponse(HotRodHeader h, Throwable t) {
      return new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
            OperationStatus.NodeSuspected, h.topologyId, t.toString());
   }

   private ErrorResponse createIllegalLifecycleStateErrorResponse(HotRodHeader h, Throwable t) {
      return new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
            OperationStatus.IllegalLifecycleState, h.topologyId, t.toString());
   }

   private ErrorResponse createServerErrorResponse(HotRodHeader h, Throwable t) {
      return new ErrorResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
            OperationStatus.ServerError, h.topologyId, createErrorMsg(t));
   }

   String createErrorMsg(Throwable t) {
      Set<Throwable> causes = new LinkedHashSet<>();
      Throwable initial = t;
      while (initial != null && !causes.contains(initial)) {
         causes.add(initial);
         initial = initial.getCause();
      }
      return causes.stream().map(Object::toString).collect(Collectors.joining("\n"));
   }

   @Override
   public AdvancedCache<byte[], byte[]> getOptimizedCache(HotRodHeader h, AdvancedCache<byte[], byte[]> c, Configuration cacheCfg) {
      boolean isTransactional = cacheCfg.transaction().transactionMode().isTransactional();
      boolean isClustered = cacheCfg.clustering().cacheMode().isClustered();

      AdvancedCache<byte[], byte[]> optCache = c;
      if (isClustered && !isTransactional && h.op.isConditional()) {
         log.warnConditionalOperationNonTransactional(h.op.toString());
      }

      if (h.op.canSkipCacheLoading() && hasFlag(h, ProtocolFlag.SkipCacheLoader)) {
         optCache = c.withFlags(Flag.SKIP_CACHE_LOAD);
      }

      if (h.op.canSkipIndexing() && hasFlag(h, ProtocolFlag.SkipIndexing)) {
         optCache = c.withFlags(Flag.SKIP_INDEXING);
      }
      if (!hasFlag(h, ProtocolFlag.ForceReturnPreviousValue)) {
         if (h.op.isNotConditionalAndCanReturnPrevious()) {
            optCache = optCache.withFlags(Flag.IGNORE_RETURN_VALUES);
         }
      } else if (!isTransactional && h.op.canReturnPreviousValue()) {
         log.warnForceReturnPreviousNonTransactional(h.op.toString());
      }
      return optCache;
   }
}

class ExecRequestContext {
   private final String name;
   private final int paramSize;
   private final Map<String, byte[]> params;

   ExecRequestContext(String name, int paramSize, Map<String, byte[]> params) {
      this.name = name;
      this.paramSize = paramSize;
      this.params = params;
   }

   public String getName() {
      return name;
   }

   public int getParamSize() {
      return paramSize;
   }

   public Map<String, byte[]> getParams() {
      return params;
   }
}

class ClientListenerRequestContext {
   private final byte[] listenerId;
   private final boolean includeCurrentState;
   private Optional<KeyValuePair<String, List<byte[]>>> filterFactoryInfo;
   private Optional<KeyValuePair<String, List<byte[]>>> converterFactoryInfo;
   private boolean useRawData;

   ClientListenerRequestContext(byte[] listenerId, boolean includeCurrentState) {
      this.listenerId = listenerId;
      this.includeCurrentState = includeCurrentState;
   }

   public byte[] getListenerId() {
      return listenerId;
   }

   public boolean isIncludeCurrentState() {
      return includeCurrentState;
   }

   public Optional<KeyValuePair<String, List<byte[]>>> getFilterFactoryInfo() {
      return filterFactoryInfo;
   }

   public void setFilterFactoryInfo(Optional<KeyValuePair<String, List<byte[]>>> filterFactoryInfo) {
      this.filterFactoryInfo = filterFactoryInfo;
   }

   public Optional<KeyValuePair<String, List<byte[]>>> getConverterFactoryInfo() {
      return converterFactoryInfo;
   }

   public void setConverterFactoryInfo(Optional<KeyValuePair<String, List<byte[]>>> converterFactoryInfo) {
      this.converterFactoryInfo = converterFactoryInfo;
   }

   public boolean isUseRawData() {
      return useRawData;
   }

   public void setUseRawData(boolean useRawData) {
      this.useRawData = useRawData;
   }
}

class IterationStartRequest {
   private final Optional<byte[]> optionBitSet;
   private final Optional<KeyValuePair<String, List<byte[]>>> factory;
   private final int batch;
   private final boolean metadata;

   IterationStartRequest(Optional<byte[]> optionBitSet, Optional<KeyValuePair<String, List<byte[]>>> factory,
                         int batch, boolean metadata) {
      this.optionBitSet = optionBitSet;
      this.factory = factory;
      this.batch = batch;
      this.metadata = metadata;
   }

   public Optional<byte[]> getOptionBitSet() {
      return optionBitSet;
   }

   public Optional<KeyValuePair<String, List<byte[]>>> getFactory() {
      return factory;
   }

   public int getBatch() {
      return batch;
   }

   public boolean isMetadata() {
      return metadata;
   }
}
