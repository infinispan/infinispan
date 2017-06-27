package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readMaybeByte;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readMaybeLong;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readMaybeOptRangedBytes;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readMaybeOptString;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readMaybeRangedBytes;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readMaybeSignedInt;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readMaybeString;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readMaybeVInt;

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
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.Flag;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.server.core.transport.ExtendedByteBufJava;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.hotrod.CacheDecodeContext.ExpirationParam;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.tx.ControlByte;
import org.infinispan.stats.ClusterCacheStats;
import org.infinispan.stats.Stats;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.concurrent.TimeoutException;
import org.jgroups.SuspectedException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.CharsetUtil;

/**
 * HotRod protocol decoder specific for specification version 2.0.
 *
 * @author Galder Zamarreño
 * @since 7.0
 */
class Decoder2x implements VersionedDecoder {

   private static final Log log = LogFactory.getLog(Decoder2x.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private static final long EXPIRATION_NONE = -1;
   private static final long EXPIRATION_DEFAULT = -2;

   private static final ExpirationParam DEFAULT_EXPIRATION = new ExpirationParam(-1, TimeUnitValue.SECONDS);

   @Override
   public boolean readHeader(ByteBuf buffer, byte version, long messageId, HotRodHeader header) throws Exception {
      if (header.op == null) {
         int readableBytes = buffer.readableBytes();
         // We require at least 2 bytes at minimum
         if (readableBytes < 2) {
            buffer.resetReaderIndex();
            return false;
         }
         byte streamOp = buffer.readByte();
         int length = ExtendedByteBufJava.readMaybeVInt(buffer);
         // Didn't have enough bytes for VInt or the length is too long for remaining
         if (length == Integer.MIN_VALUE || length > buffer.readableBytes()) {
            buffer.resetReaderIndex();
            return false;
         } else if (length == 0) {
            header.cacheName = "";
         } else {
            byte[] bytes = new byte[length];
            buffer.readBytes(bytes);
            header.cacheName = new String(bytes, CharsetUtil.UTF_8);
         }
         header.op = HotRodOperation.fromRequestOpCode(streamOp);
         if (header.op == null) {
            throw new HotRodUnknownOperationException("Unknown operation: " + streamOp, version, messageId);
         }
         buffer.markReaderIndex();
      }
      int flag = ExtendedByteBufJava.readMaybeVInt(buffer);
      if (flag == Integer.MIN_VALUE) {
         return false;
      }
      if (buffer.readableBytes() < 2) {
         buffer.resetReaderIndex();
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
         case REMOVE_IF_UNMODIFIED:
            return readParameters(buffer, header, false, false, true);
         case REPLACE_IF_UNMODIFIED:
            return readParameters(buffer, header, true, true, true);
         case GET_ALL:
            return readParameters(buffer, header, false, true, false);
         case PUT_STREAM:
            return readParameters(buffer, header, true, false, true);
         default:
            return readParameters(buffer, header, true, true, false);
      }
   }

   private static CacheDecodeContext.RequestParameters readParameters(ByteBuf buffer, HotRodHeader header, boolean readExpiration,
                                                                      boolean readSize, boolean readVersion) {
      ExpirationParam param1;
      ExpirationParam param2;
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
         if (version == Long.MIN_VALUE) {
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

   private static ExpirationParam readExpirationParam(boolean pre22Version, boolean useDefault, ByteBuf buffer,
                                                      byte timeUnit) {
      if (pre22Version) {
         int duration = ExtendedByteBufJava.readMaybeVInt(buffer);
         if (duration == Integer.MIN_VALUE) {
            return null;
         } else if (duration <= 0) {
            duration = useDefault ? (int) EXPIRATION_DEFAULT : (int) EXPIRATION_NONE;
         }
         return new ExpirationParam(duration, TimeUnitValue.decode(timeUnit));
      } else {
         switch (timeUnit) {
            // Default time unit
            case 0x07:
               return new ExpirationParam(EXPIRATION_DEFAULT, TimeUnitValue.decode(timeUnit));
            // Infinite time unit
            case 0x08:
               return new ExpirationParam(EXPIRATION_NONE, TimeUnitValue.decode(timeUnit));
            default:
               long timeDuration = ExtendedByteBufJava.readMaybeVLong(buffer);
               if (timeDuration == Long.MIN_VALUE) {
                  return null;
               }
               return new ExpirationParam(timeDuration, TimeUnitValue.decode(timeUnit));
         }
      }
   }

   private static boolean hasFlag(HotRodHeader h, ProtocolFlag f) {
      return (h.flag & f.getValue()) == f.getValue();
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
      if (hasFlag(h, ProtocolFlag.ForceReturnPreviousValue)) {
         switch (st) {
            case Success:
               switch (h.op) {
                  case PUT:
                  case REPLACE:
                  case REMOVE_IF_UNMODIFIED:
                  case REMOVE:
                  case REPLACE_IF_UNMODIFIED:
                     return new ResponseWithPrevious(h.version, h.messageId, h.cacheName,
                           h.clientIntel, h.op, OperationStatus.SuccessWithPrevious, h.topologyId, Optional.ofNullable(prev));
               }
               break;
            case OperationNotExecuted:
               switch (h.op) {
                  case PUT_IF_ABSENT:
                  case REPLACE_IF_UNMODIFIED:
                  case REMOVE_IF_UNMODIFIED:
                     return new ResponseWithPrevious(h.version, h.messageId, h.cacheName,
                           h.clientIntel, h.op, OperationStatus.NotExecutedWithPrevious, h.topologyId, Optional.ofNullable(prev));
               }
               break;
         }
      }
      return new EmptyResponse(h.version, h.messageId, h.cacheName, h.clientIntel, h.op, st, h.topologyId);
   }

   @Override
   public Response createGetResponse(HotRodHeader h, CacheEntry<byte[], byte[]> entry) {
      HotRodOperation op = h.op;
      if (entry != null) {
         switch (op) {
            case GET:
               return new GetResponse(h.version, h.messageId, h.cacheName, h.clientIntel, HotRodOperation.GET,
                     OperationStatus.Success, h.topologyId, entry.getValue());
            case GET_WITH_VERSION:
               long version;
               NumericVersion numericVersion = (NumericVersion) entry.getMetadata().version();
               if (numericVersion != null) {
                  version = numericVersion.getVersion();
               } else {
                  version = 0;
               }
               return new GetWithVersionResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
                     HotRodOperation.GET_WITH_VERSION, OperationStatus.Success, h.topologyId, entry.getValue(),
                     version);
         }
      } else {
         switch (op) {
            case GET:
               return new GetResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
                     HotRodOperation.GET, OperationStatus.KeyDoesNotExist, h.topologyId, null);
            case GET_WITH_VERSION:
               return new GetWithVersionResponse(h.version, h.messageId, h.cacheName,
                     h.clientIntel, HotRodOperation.GET_WITH_VERSION, OperationStatus.KeyDoesNotExist,
                     h.topologyId, null, 0);
         }
      }
      throw new IllegalStateException("Unreachable code");
   }

   @Override
   public void customReadHeader(HotRodHeader header, ByteBuf buffer, CacheDecodeContext hrCtx, List<Object> out) {
      switch (header.op) {
         case AUTH:
            readMaybeString(buffer).flatMap(mech ->
                  readMaybeRangedBytes(buffer).map(clientResponse -> {
                     hrCtx.operationDecodeContext = new KeyValuePair<>(mech, clientResponse);
                     buffer.markReaderIndex();
                     out.add(hrCtx);
                     return null;
                  }));
            break;
         case EXEC:
            ExecRequestContext execCtx = (ExecRequestContext) hrCtx.operationDecodeContext;
            // first time read
            if (execCtx == null) {
               Optional<ExecRequestContext> optional = readMaybeString(buffer).flatMap(name ->
                     readMaybeVInt(buffer).map(paramCount -> {
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
                  if (!readMaybeString(buffer).flatMap(key ->
                        readMaybeRangedBytes(buffer).map(value -> {
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
         case PREPARE_TX:
            PrepareTransactionContext ctx = (PrepareTransactionContext) hrCtx.operationDecodeContext;
            if (ctx == null) {
               Optional<PrepareTransactionContext> optCtx = readPrepareTxContext(buffer);
               if (optCtx.isPresent()) {
                  ctx = optCtx.get();
                  hrCtx.operationDecodeContext = ctx;
                  buffer.markReaderIndex();
                  if (trace) {
                     log.tracef("Decoded Prepare Transaction Context: %s", ctx);
                  }
               } else {
                  return;
               }
            }
            while (!ctx.isFinished()) {
               Optional<TransactionWrite> txWrite = readTransactionWrite(buffer);
               if (txWrite.isPresent()) {
                  TransactionWrite write = txWrite.get();
                  ctx.addWrite(write);
                  buffer.markReaderIndex();
                  if (trace) {
                     log.tracef("Decoded Transaction Write: %s", write);
                  }
               } else {
                  return;
               }
            }
            out.add(hrCtx);
            break;
         case COMMIT_TX:
         case ROLLBACK_TX:
            readXid(buffer).ifPresent(xid -> {
               hrCtx.operationDecodeContext = xid;
               out.add(hrCtx);
               buffer.markReaderIndex();
               if (trace) {
                  log.tracef("Decoded commit/rollback XID: %s", xid);
               }
            });
            break;
         default:
            // This operation doesn't need additional reads - has everything to process
            out.add(hrCtx);
      }
   }

   private Optional<PrepareTransactionContext> readPrepareTxContext(ByteBuf buffer) {
      return readXid(buffer)
            .flatMap(xid -> readMaybeByte(buffer)
                  .flatMap(onePhaseCommit -> readMaybeVInt(buffer)
                        .map(numberOfWrites ->
                              new PrepareTransactionContext(xid, onePhaseCommit == 1, numberOfWrites))));
   }

   private Optional<TransactionWrite> readTransactionWrite(ByteBuf byteBuf) {
      return readMaybeRangedBytes(byteBuf).flatMap(key ->
            readMaybeByte(byteBuf).flatMap(control -> {
               boolean noVersion = ControlByte.NON_EXISTING.hasFlag(control) || ControlByte.NOT_READ.hasFlag(control);
               Optional<Long> versionOptional = noVersion ? Optional.of(0L) : readMaybeLong(byteBuf);
               if (ControlByte.REMOVE_OP.hasFlag(control)) {
                  return versionOptional.map(version ->
                        new TransactionWrite(key, version, control, null, null, null));
               } else {
                  return versionOptional.flatMap(version ->
                        readWriteExpirationAndValue(byteBuf, key, version, control));
               }
            }));
   }

   private Optional<TransactionWrite> readWriteExpirationAndValue(ByteBuf byteBuf, byte[] key,
         long version, byte control) {
      return readLifespanAndMaxidle(byteBuf).flatMap(parameters ->
            readMaybeRangedBytes(byteBuf).map(value ->
                  new TransactionWrite(key, version, control, parameters.lifespan, parameters.maxIdle, value)));
   }

   private Optional<ExpirationParam> readExpirationParam(ByteBuf byteBuf, byte timeUnit) {
      switch (timeUnit) {
         // Default time unit
         case 0x07:
            return Optional.of(new ExpirationParam(EXPIRATION_DEFAULT, TimeUnitValue.decode(timeUnit)));
         // Infinite time unit
         case 0x08:
            return Optional.of(new ExpirationParam(EXPIRATION_NONE, TimeUnitValue.decode(timeUnit)));
         default:
            return readMaybeLong(byteBuf).map(time -> new ExpirationParam(time, TimeUnitValue.decode(timeUnit)));
      }
   }

   private Optional<XidImpl> readXid(ByteBuf byteBuf) {
      return readMaybeSignedInt(byteBuf)
            .flatMap(formatId -> readMaybeRangedBytes(byteBuf)
                  .flatMap(gtxId -> readMaybeRangedBytes(byteBuf)
                        .map(branchId -> XidImpl.create(formatId, gtxId, branchId))));
   }

   private Optional<CacheDecodeContext.RequestParameters> readLifespanAndMaxidle(ByteBuf byteBuf) {
      return readMaybeByte(byteBuf).flatMap(units -> {
         final byte firstUnit = (byte) ((units & 0xf0) >> 4);
         final byte secondUnit = (byte) (units & 0x0f);
         return readExpirationParam(byteBuf, firstUnit).flatMap(lifespan ->
               readExpirationParam(byteBuf, secondUnit).map(maxIdle ->
                     new CacheDecodeContext.RequestParameters(0, lifespan, maxIdle, 0)));
      });
   }

   @Override
   public void customReadKey(HotRodHeader header, ByteBuf buffer, CacheDecodeContext hrCtx, List<Object> out) {
      switch (header.op) {
         case BULK_GET:
         case BULK_GET_KEYS:
            readMaybeVInt(buffer).ifPresent(number -> {
               hrCtx.operationDecodeContext = number;
               buffer.markReaderIndex();
               out.add(hrCtx);
            });
            break;
         case QUERY:
            readMaybeRangedBytes(buffer).ifPresent(query -> {
               hrCtx.operationDecodeContext = query;
               buffer.markReaderIndex();
               out.add(hrCtx);
            });
            break;
         case GET_STREAM:
            readMaybeVInt(buffer).ifPresent(offset -> {
               hrCtx.operationDecodeContext = offset;
               buffer.markReaderIndex();
               out.add(hrCtx);
            });
            break;
         case ADD_CLIENT_LISTENER:
            ClientListenerRequestContext requestCtx;
            if (hrCtx.operationDecodeContext == null) {
               Optional<ClientListenerRequestContext> optional = readMaybeRangedBytes(buffer).flatMap(listenerId ->
                     readMaybeByte(buffer).map(includeState -> {
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
            if (requestCtx.getConverterFactoryInfo() == null) {
               if (!readMaybeNamedFactory(buffer).map(converter -> {
                  boolean useRawData;
                  if (Constants.isVersion2x(header.version)) {
                     Optional<Byte> rawOptional = readMaybeByte(buffer);
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

                  return requestCtx;
               }).isPresent()) {
                  return;
               }
            }
            if (Constants.isVersionPost25(header.version)) {
               int listenerInterests = ExtendedByteBufJava.readMaybeVInt(buffer);
               if (listenerInterests == Integer.MIN_VALUE)
                  return;

               requestCtx.setListenerInterests(listenerInterests);
               buffer.markReaderIndex();
            }
            out.add(hrCtx);
            break;
         case REMOVE_CLIENT_LISTENER:
            readMaybeRangedBytes(buffer).ifPresent(listenerId -> {
               hrCtx.operationDecodeContext = listenerId;
               buffer.markReaderIndex();
               out.add(hrCtx);
            });
            break;
         case ITERATION_START:
            readMaybeOptRangedBytes(buffer).flatMap(segments ->
                  readMaybeOptString(buffer).map(name -> {
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
                     Optional<Integer> optionalBatchSize = readMaybeVInt(buffer);
                     if (optionalBatchSize.isPresent()) {
                        batchSize = optionalBatchSize.get();
                     } else {
                        return null;
                     }
                     boolean metadata;
                     if (isPre24) {
                        metadata = false;
                     } else {
                        Optional<Byte> optionalMetadata = readMaybeByte(buffer);
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
            break;
         case ITERATION_NEXT:
         case ITERATION_END:
            readMaybeString(buffer).ifPresent(iterationId -> {
               hrCtx.operationDecodeContext = iterationId;
               buffer.markReaderIndex();
               out.add(hrCtx);
            });
            break;
      }
   }

   private Optional<Optional<KeyValuePair<String, List<byte[]>>>> readMaybeNamedFactory(ByteBuf buffer) {
      return readMaybeString(buffer).flatMap(name -> {
         if (!name.isEmpty()) {
            return readOptionalParams(buffer).map(param -> Optional.of(new KeyValuePair<>(name, param)));
         } else return Optional.of(Optional.empty());
      });
   }

   private Optional<List<byte[]>> readOptionalParams(ByteBuf buffer) {
      Optional<Byte> numParams = readMaybeByte(buffer);
      return numParams.map(p -> {
         if (p > 0) {
            List<byte[]> params = new ArrayList<>();
            boolean readAll = true;
            while (params.size() < p) {
               if (!readMaybeRangedBytes(buffer).map(param -> {
                  params.add(param);
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

   @Override
   public void customReadValue(HotRodHeader header, ByteBuf buffer, CacheDecodeContext hrCtx, List<Object> out) {
      switch (header.op) {
         case PUT_ALL:
            int maxLength = hrCtx.params.valueLength;
            Map<byte[], byte[]> map;
            if (hrCtx.operationDecodeContext == null) {
               map = new HashMap<>(maxLength);
               hrCtx.operationDecodeContext = map;
            } else {
               //noinspection unchecked
               map = (Map<byte[], byte[]>) hrCtx.operationDecodeContext;
            }
            boolean readAll = true;
            while (map.size() < maxLength) {
               if (!readMaybeRangedBytes(buffer).flatMap(key ->
                     readMaybeRangedBytes(buffer).map(value -> {
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
         case GET_ALL:
            maxLength = hrCtx.params.valueLength;
            Set<byte[]> set;
            if (hrCtx.operationDecodeContext == null) {
               set = new HashSet<>(maxLength);
               hrCtx.operationDecodeContext = set;
            } else {
               //noinspection unchecked
               set = (Set<byte[]>) hrCtx.operationDecodeContext;
            }
            readAll = true;
            while (set.size() < maxLength) {
               if (!readMaybeRangedBytes(buffer).map(bytes -> {
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
         case PUT_STREAM:
            ByteBuf vBuffer;
            if (hrCtx.operationDecodeContext == null) {
               hrCtx.operationDecodeContext = vBuffer = ByteBufAllocator.DEFAULT.buffer();
            } else {
               vBuffer = (ByteBuf) hrCtx.operationDecodeContext;
            }
            if (vBuffer != null) {
               readMaybeRangedBytes(buffer).map(bytes -> {
                  if (bytes.length > 0) {
                     vBuffer.writeBytes(bytes);
                  } else {
                     out.add(hrCtx);
                  }
                  buffer.markReaderIndex();
                  return Optional.empty();
               });
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
         Throwable cause = t.getCause() == null ? t : t.getCause();
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

abstract class NamedParametricRequestContext<T> {
   private final String name;
   private final int paramSize;
   private final Map<String, T> params;

   NamedParametricRequestContext(String name, int paramSize, Map<String, T> params) {
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

   public Map<String, T> getParams() {
      return params;
   }
}

class ExecRequestContext extends NamedParametricRequestContext<byte[]> {
   ExecRequestContext(String name, int paramSize, Map<String, byte[]> params) {
      super(name, paramSize, params);
   }
}

class AdminRequestContext extends NamedParametricRequestContext<String> {
   AdminRequestContext(String name, int paramSize, Map<String, String> params) {
      super(name, paramSize, params);
   }
}

class ClientListenerRequestContext {
   private final byte[] listenerId;
   private final boolean includeCurrentState;
   private Optional<KeyValuePair<String, List<byte[]>>> filterFactoryInfo;
   private Optional<KeyValuePair<String, List<byte[]>>> converterFactoryInfo;
   private boolean useRawData;
   private int listenerInterests;

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

   public int getListenerInterests() {
      return listenerInterests;
   }

   public void setListenerInterests(int listenerInterests) {
      this.listenerInterests = listenerInterests;
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

class PrepareTransactionContext {
   private final XidImpl xid;
   private final boolean onePhaseCommit;
   private final int numberOfWrites;
   private final List<TransactionWrite> writeOperations;

   PrepareTransactionContext(XidImpl xid, boolean onePhaseCommit, int numberOfWrites) {
      this.xid = xid;
      this.onePhaseCommit = onePhaseCommit;
      this.numberOfWrites = numberOfWrites;
      this.writeOperations = new ArrayList<>(numberOfWrites);
   }

   public boolean isFinished() {
      return numberOfWrites == writeOperations.size();
   }

   public boolean isEmpty() {
      return writeOperations.isEmpty();
   }

   public XidImpl getXid() {
      return xid;
   }

   @Override
   public String toString() {
      return "PrepareTransactionContext{" +
            "xid=" + xid +
            ", onePhaseCommit=" + onePhaseCommit +
            ", numberOfWrites=" + numberOfWrites +
            '}';
   }

   boolean isOnePhaseCommit() {
      return onePhaseCommit;
   }

   List<TransactionWrite> writes() {
      return writeOperations;
   }

   void addWrite(TransactionWrite transactionWrite) {
      writeOperations.add(transactionWrite);
   }
}

class TransactionWrite {
   final byte[] key;
   final long versionRead;
   final CacheDecodeContext.ExpirationParam lifespan;
   final CacheDecodeContext.ExpirationParam maxIdle;
   final byte[] value;
   private final byte control;

   TransactionWrite(byte[] key, long versionRead, byte control, CacheDecodeContext.ExpirationParam lifespan,
         CacheDecodeContext.ExpirationParam maxIdle, byte[] value) {
      this.key = key;
      this.versionRead = versionRead;
      this.control = control;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.value = value;
   }

   public boolean isRemove() {
      return ControlByte.REMOVE_OP.hasFlag(control);
   }

   @Override
   public String toString() {
      return "TransactionWrite{" +
            "key=" + Util.printArray(key, true) +
            ", versionRead=" + versionRead +
            ", control=" + ControlByte.prettyPrint(control) +
            ", lifespan=" + lifespan +
            ", maxIdle=" + maxIdle +
            ", value=" + Util.printArray(value, true) +
            '}';
   }

   boolean skipRead() {
      return ControlByte.NOT_READ.hasFlag(control);
   }

   boolean wasNonExisting() {
      return ControlByte.NON_EXISTING.hasFlag(control);
   }
}
