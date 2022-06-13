package org.infinispan.hotrod.impl.protocol;

import static org.infinispan.hotrod.impl.TimeUnitParam.encodeTimeUnits;
import static org.infinispan.hotrod.impl.logging.Log.HOTROD;
import static org.infinispan.hotrod.impl.transport.netty.ByteBufUtil.limitedHexDump;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.BitSet;
import java.util.EnumSet;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntConsumer;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.events.cache.CacheEntryEventType;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.MediaTypeIds;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.counter.api.CounterState;
import org.infinispan.hotrod.configuration.ClientIntelligence;
import org.infinispan.hotrod.event.ClientEvent;
import org.infinispan.hotrod.event.ClientListener;
import org.infinispan.hotrod.event.impl.AbstractClientEvent;
import org.infinispan.hotrod.event.impl.CreatedEventImpl;
import org.infinispan.hotrod.event.impl.CustomEventImpl;
import org.infinispan.hotrod.event.impl.ExpiredEventImpl;
import org.infinispan.hotrod.event.impl.ModifiedEventImpl;
import org.infinispan.hotrod.event.impl.RemovedEventImpl;
import org.infinispan.hotrod.exceptions.HotRodClientException;
import org.infinispan.hotrod.exceptions.RemoteIllegalLifecycleStateException;
import org.infinispan.hotrod.exceptions.RemoteNodeSuspectException;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.cache.RemoteCache;
import org.infinispan.hotrod.impl.counter.HotRodCounterEvent;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.operations.AbstractKeyOperation;
import org.infinispan.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.hotrod.impl.operations.PingResponse;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.ChannelFactory;

import io.netty.buffer.ByteBuf;

/**
 * @since 14.0
 */
public class Codec40 implements Codec, HotRodConstants {

   static final Log log = LogFactory.getLog(Codec.class, Log.class);

   public static final String EMPTY_VALUE_CONVERTER = "org.infinispan.server.hotrod.HotRodServer$ToEmptyBytesKeyValueFilterConverter";

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      HeaderParams headerParams = writeHeader(buf, params, HotRodConstants.VERSION_40);
      writeDataTypes(buf, params.dataFormat);
      return headerParams;
   }

   protected void writeDataTypes(ByteBuf buf, DataFormat dataFormat) {
      MediaType keyType = null, valueType = null;
      if (dataFormat != null) {
         keyType = dataFormat.getKeyType();
         valueType = dataFormat.getValueType();
      }
      writeMediaType(buf, keyType);
      writeMediaType(buf, valueType);
   }

   private void writeMediaType(ByteBuf buf, MediaType mediaType) {
      if (mediaType == null) {
         buf.writeByte(0);
      } else {
         Short id = MediaTypeIds.getId(mediaType);
         if (id != null) {
            buf.writeByte(1);
            ByteBufUtil.writeVInt(buf, id);
         } else {
            buf.writeByte(2);
            ByteBufUtil.writeString(buf, mediaType.toString());
         }
         Map<String, String> parameters = mediaType.getParameters();
         ByteBufUtil.writeVInt(buf, parameters.size());
         parameters.forEach((key, value) -> {
            ByteBufUtil.writeString(buf, key);
            ByteBufUtil.writeString(buf, value);
         });
      }
   }

   @Override
   public void writeClientListenerInterests(ByteBuf buf, EnumSet<CacheEntryEventType> types) {
      byte listenerInterests = 0;
      if (types.contains(CacheEntryEventType.CREATED))
         listenerInterests = (byte) (listenerInterests | 0x01);
      if (types.contains(CacheEntryEventType.UPDATED))
         listenerInterests = (byte) (listenerInterests | 0x02);
      if (types.contains(CacheEntryEventType.REMOVED))
         listenerInterests = (byte) (listenerInterests | 0x04);
      if (types.contains(CacheEntryEventType.EXPIRED))
         listenerInterests = (byte) (listenerInterests | 0x08);

      ByteBufUtil.writeVInt(buf, listenerInterests);
   }

   @Override
   public void writeClientListenerParams(ByteBuf buf, ClientListener clientListener,
         byte[][] filterFactoryParams, byte[][] converterFactoryParams) {
      buf.writeByte((short) (clientListener.includeCurrentState() ? 1 : 0));
      writeNamedFactory(buf, clientListener.filterFactoryName(), filterFactoryParams);
      writeNamedFactory(buf, clientListener.converterFactoryName(), converterFactoryParams);
      buf.writeByte((short) (clientListener.useRawData() ? 1 : 0));
   }

   @Override
   public void writeExpirationParams(ByteBuf buf, CacheEntryExpiration.Impl expiration) {
      byte timeUnits = encodeTimeUnits(expiration);
      buf.writeByte(timeUnits);
      Duration lifespan = expiration.rawLifespan();
      if (lifespan != null && lifespan != Duration.ZERO) {
         ByteBufUtil.writeVLong(buf, lifespan.toSeconds());
      }
      Duration maxIdle = expiration.rawMaxIdle();
      if (maxIdle != null && lifespan != Duration.ZERO) {
         ByteBufUtil.writeVLong(buf, maxIdle.toSeconds());
      }
   }

   @Override
   public void writeBloomFilter(ByteBuf buf, int bloomFilterBits) {
      ByteBufUtil.writeVInt(buf, bloomFilterBits);
   }

   @Override
   public int estimateExpirationSize(CacheEntryExpiration.Impl expiration) {
      int lifespanSeconds = durationToSeconds(expiration.rawLifespan());
      int maxIdleSeconds = durationToSeconds(expiration.rawMaxIdle());
      return 1 + (lifespanSeconds > 0 ? ByteBufUtil.estimateVLongSize(lifespanSeconds) : 0) + (maxIdleSeconds > 0 ? ByteBufUtil.estimateVLongSize(maxIdleSeconds) : 0);
   }

   private int durationToSeconds(Duration duration) {
      return duration == null ? 0 : (int) duration.toSeconds();
   }

   private void writeNamedFactory(ByteBuf buf, String factoryName, byte[][] params) {
      ByteBufUtil.writeString(buf, factoryName);
      if (!factoryName.isEmpty()) {
         // A named factory was written, how many parameters?
         if (params != null) {
            buf.writeByte((short) params.length);
            for (byte[] param : params)
               ByteBufUtil.writeArray(buf, param);
         } else {
            buf.writeByte((short) 0);
         }
      }
   }

   protected HeaderParams writeHeader(ByteBuf buf, HeaderParams params, byte version) {
      buf.writeByte(HotRodConstants.REQUEST_MAGIC);
      ByteBufUtil.writeVLong(buf, params.messageId);
      buf.writeByte(version);
      buf.writeByte(params.opCode);
      ByteBufUtil.writeArray(buf, params.cacheName);
      ByteBufUtil.writeVInt(buf, params.flags);
      buf.writeByte(params.clientIntel);
      int topologyId = params.topologyId.get();
      ByteBufUtil.writeVInt(buf, topologyId);

      if (log.isTraceEnabled())
         log.tracef("[%s] Wrote header for messageId=%d. Operation code: %#04x(%s). Flags: %#x. Topology id: %s",
               new String(params.cacheName), params.messageId, params.opCode,
               Names.of(params.opCode), params.flags, topologyId);

      return params;
   }

   @Override
   public int estimateHeaderSize(HeaderParams params) {
      return 1 + ByteBufUtil.estimateVLongSize(params.messageId) + 1 + 1 +
            ByteBufUtil.estimateArraySize(params.cacheName) + ByteBufUtil.estimateVIntSize(params.flags) +
            1 + 1 + ByteBufUtil.estimateVIntSize(params.topologyId.get());
   }

   public long readMessageId(ByteBuf buf) {
      short magic = buf.readUnsignedByte();
      if (magic != HotRodConstants.RESPONSE_MAGIC) {

         if (log.isTraceEnabled())
            log.tracef("Socket dump: %s", limitedHexDump(buf));
         throw HOTROD.invalidMagicNumber(HotRodConstants.RESPONSE_MAGIC, magic);
      }
      return ByteBufUtil.readVLong(buf);
   }

   @Override
   public short readOpCode(ByteBuf buf) {
      return buf.readUnsignedByte();
   }

   @Override
   public short readHeader(ByteBuf buf, double receivedOpCode, HeaderParams params, ChannelFactory channelFactory, SocketAddress serverAddress) {
      // Read both the status and new topology (if present),
      // before deciding how to react to error situations.
      short status = buf.readUnsignedByte();
      readNewTopologyIfPresent(buf, params, channelFactory);

      // Now that all headers values have been read, check the error responses.
      // This avoids situations where an exceptional return ends up with
      // the socket containing data from previous request responses.
      if (receivedOpCode != params.opRespCode) {
         if (receivedOpCode == HotRodConstants.ERROR_RESPONSE) {
            checkForErrorsInResponseStatus(buf, params, status, serverAddress);
         }
         throw HOTROD.invalidResponse(new String(params.cacheName), params.opRespCode, receivedOpCode);
      }

      return status;
   }

   private static CounterState decodeOldState(short encoded) {
      switch (encoded & 0x03) {
         case 0:
            return CounterState.VALID;
         case 0x01:
            return CounterState.LOWER_BOUND_REACHED;
         case 0x02:
            return CounterState.UPPER_BOUND_REACHED;
         default:
            throw new IllegalStateException();
      }
   }

   private static CounterState decodeNewState(short encoded) {
      switch (encoded & 0x0C) {
         case 0:
            return CounterState.VALID;
         case 0x04:
            return CounterState.LOWER_BOUND_REACHED;
         case 0x08:
            return CounterState.UPPER_BOUND_REACHED;
         default:
            throw new IllegalStateException();
      }
   }

   @Override
   public HotRodCounterEvent readCounterEvent(ByteBuf buf) {
      short status = buf.readByte();
      assert status == 0;
      short topology = buf.readByte();
      assert topology == 0;
      String counterName = ByteBufUtil.readString(buf);
      byte[] listenerId = ByteBufUtil.readArray(buf);
      short encodedCounterState = buf.readByte();
      long oldValue = buf.readLong();
      long newValue = buf.readLong();
      return new HotRodCounterEvent(listenerId, counterName, oldValue, decodeOldState(encodedCounterState), newValue,
            decodeNewState(encodedCounterState));
   }

   @Override
   public <K> CloseableIterator<K> keyIterator(RemoteCache<K, ?> remoteCache, CacheOperationsFactory cacheOperationsFactory,
         CacheOptions options, IntSet segments, int batchSize) {
      return new IteratorMapper<>(remoteCache.retrieveEntries(
            // Use the ToEmptyBytesKeyValueFilterConverter to remove value payload
            EMPTY_VALUE_CONVERTER, segments, batchSize), e -> (K) e.key());
   }

   @Override
   public <K, V> CloseableIterator<CacheEntry<K, V>> entryIterator(RemoteCache<K, V> remoteCache, IntSet segments,
         int batchSize) {
      return castEntryIterator(remoteCache.retrieveEntries(null, segments, batchSize));
   }

   protected <K, V> CloseableIterator<CacheEntry<K, V>> castEntryIterator(CloseableIterator iterator) {
      return iterator;
   }

   @Override
   public boolean isObjectStorageHinted(PingResponse pingResponse) {
      return pingResponse.isObjectStorage();
   }

   @Override
   public AbstractClientEvent readCacheEvent(ByteBuf buf, Function<byte[], DataFormat> listenerDataFormat, short eventTypeId, ClassAllowList allowList, SocketAddress serverAddress) {
      short status = buf.readUnsignedByte();
      buf.readUnsignedByte(); // ignore, no topology expected
      ClientEvent.Type eventType;
      switch (eventTypeId) {
         case CACHE_ENTRY_CREATED_EVENT_RESPONSE:
            eventType = ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED;
            break;
         case CACHE_ENTRY_MODIFIED_EVENT_RESPONSE:
            eventType = ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED;
            break;
         case CACHE_ENTRY_REMOVED_EVENT_RESPONSE:
            eventType = ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED;
            break;
         case CACHE_ENTRY_EXPIRED_EVENT_RESPONSE:
            eventType = ClientEvent.Type.CLIENT_CACHE_ENTRY_EXPIRED;
            break;
         case ERROR_RESPONSE:
            checkForErrorsInResponseStatus(buf, null, status, serverAddress);
         default:
            throw HOTROD.unknownEvent(eventTypeId);
      }

      byte[] listenerId = ByteBufUtil.readArray(buf);
      short isCustom = buf.readUnsignedByte();
      boolean isRetried = buf.readUnsignedByte() == 1;
      DataFormat dataFormat = listenerDataFormat.apply(listenerId);

      if (isCustom == 1) {
         final Object eventData = dataFormat.valueToObj(ByteBufUtil.readArray(buf), allowList);
         return createCustomEvent(listenerId, eventData, eventType, isRetried);
      } else if (isCustom == 2) { // New in 2.1, dealing with raw custom events
         return createCustomEvent(listenerId, ByteBufUtil.readArray(buf), eventType, isRetried); // Raw data
      } else {
         switch (eventType) {
            case CLIENT_CACHE_ENTRY_CREATED:
               Object createdKey = dataFormat.keyToObj(ByteBufUtil.readArray(buf), allowList);
               long createdDataVersion = buf.readLong();
               return createCreatedEvent(listenerId, createdKey, createdDataVersion, isRetried);
            case CLIENT_CACHE_ENTRY_MODIFIED:
               Object modifiedKey = dataFormat.keyToObj(ByteBufUtil.readArray(buf), allowList);
               long modifiedDataVersion = buf.readLong();
               return createModifiedEvent(listenerId, modifiedKey, modifiedDataVersion, isRetried);
            case CLIENT_CACHE_ENTRY_REMOVED:
               Object removedKey = dataFormat.keyToObj(ByteBufUtil.readArray(buf), allowList);
               return createRemovedEvent(listenerId, removedKey, isRetried);
            case CLIENT_CACHE_ENTRY_EXPIRED:
               Object expiredKey = dataFormat.keyToObj(ByteBufUtil.readArray(buf), allowList);
               return createExpiredEvent(listenerId, expiredKey);
            default:
               throw HOTROD.unknownEvent(eventTypeId);
         }
      }
   }

   protected AbstractClientEvent createExpiredEvent(byte[] listenerId, final Object key) {
      return new ExpiredEventImpl<>(listenerId, key);
   }

   @Override
   public <K, V> CacheEntry<K, V> returnPossiblePrevValue(K key, ByteBuf buf, short status, DataFormat dataFormat, int flags, ClassAllowList allowList, Marshaller marshaller) {
      if (HotRodConstants.hasPrevious(status)) {
         return AbstractKeyOperation.readEntry(buf, key, dataFormat, allowList);
      } else {
         return null;
      }
   }

   protected AbstractClientEvent createRemovedEvent(byte[] listenerId, final Object key, final boolean isRetried) {
      return new RemovedEventImpl<>(listenerId, key, isRetried);
   }

   protected AbstractClientEvent createModifiedEvent(byte[] listenerId, final Object key, final long dataVersion, final boolean isRetried) {
      return new ModifiedEventImpl<>(listenerId, key, dataVersion, isRetried);
   }

   protected AbstractClientEvent createCreatedEvent(byte[] listenerId, final Object key, final long dataVersion, final boolean isRetried) {
      return new CreatedEventImpl<>(listenerId, key, dataVersion, isRetried);
   }

   protected AbstractClientEvent createCustomEvent(byte[] listenerId, final Object eventData, final ClientEvent.Type eventType, final boolean isRetried) {
      return new CustomEventImpl<>(listenerId, eventData, isRetried, eventType);
   }

   protected void checkForErrorsInResponseStatus(ByteBuf buf, HeaderParams params, short status, SocketAddress serverAddress) {
      if (log.isTraceEnabled()) log.tracef("[%s] Received operation status: %#x", new String(params.cacheName), status);

      String msgFromServer;
      try {
         switch (status) {
            case HotRodConstants.INVALID_MAGIC_OR_MESSAGE_ID_STATUS:
            case HotRodConstants.REQUEST_PARSING_ERROR_STATUS:
            case HotRodConstants.UNKNOWN_COMMAND_STATUS:
            case HotRodConstants.SERVER_ERROR_STATUS:
            case HotRodConstants.COMMAND_TIMEOUT_STATUS:
            case HotRodConstants.UNKNOWN_VERSION_STATUS: {
               // If error, the body of the message just contains a message
               msgFromServer = ByteBufUtil.readString(buf);
               if (status == HotRodConstants.COMMAND_TIMEOUT_STATUS && log.isTraceEnabled()) {
                  log.tracef("Server-side timeout performing operation: %s", msgFromServer);
               } else {
                  HOTROD.errorFromServer(msgFromServer);
               }
               throw new HotRodClientException(msgFromServer, params.messageId, status);
            }
            case HotRodConstants.ILLEGAL_LIFECYCLE_STATE:
               msgFromServer = ByteBufUtil.readString(buf);
               throw new RemoteIllegalLifecycleStateException(msgFromServer, params.messageId, status, serverAddress);
            case HotRodConstants.NODE_SUSPECTED:
               // Handle both Infinispan's and JGroups' suspicions
               msgFromServer = ByteBufUtil.readString(buf);
               if (log.isTraceEnabled())
                  log.tracef("[%s] A remote node was suspected while executing messageId=%d. " +
                              "Check if retry possible. Message from server: %s",
                        new String(params.cacheName), params.messageId, msgFromServer);

               throw new RemoteNodeSuspectException(msgFromServer, params.messageId, status);
            default: {
               throw new IllegalStateException(String.format("Unknown status: %#04x", status));
            }
         }
      } finally {
         // Errors related to protocol parsing are odd, and they can sometimes
         // be the consequence of previous errors, so whenever these errors
         // occur, invalidate the underlying transport instance so that a
         // brand new connection is established next time around.
         switch (status) {
            case HotRodConstants.INVALID_MAGIC_OR_MESSAGE_ID_STATUS:
            case HotRodConstants.REQUEST_PARSING_ERROR_STATUS:
            case HotRodConstants.UNKNOWN_COMMAND_STATUS:
            case HotRodConstants.UNKNOWN_VERSION_STATUS: {
               // invalidation happens due to exception in operation
            }
         }
      }
   }

   protected void readNewTopologyIfPresent(ByteBuf buf, HeaderParams params, ChannelFactory channelFactory) {
      short topologyChangeByte = buf.readUnsignedByte();
      if (topologyChangeByte == 1)
         readNewTopologyAndHash(buf, params, channelFactory);
   }

   protected void readNewTopologyAndHash(ByteBuf buf, HeaderParams params, ChannelFactory channelFactory) {
      int newTopologyId = ByteBufUtil.readVInt(buf);

      InetSocketAddress[] addresses = readTopology(buf);

      final short hashFunctionVersion;
      final SocketAddress[][] segmentOwners;
      if (params.clientIntel == ClientIntelligence.HASH_DISTRIBUTION_AWARE.getValue()) {
         // Only read the hash if we asked for it
         hashFunctionVersion = buf.readUnsignedByte();
         int numSegments = ByteBufUtil.readVInt(buf);
         segmentOwners = new SocketAddress[numSegments][];
         if (hashFunctionVersion > 0) {
            for (int i = 0; i < numSegments; i++) {
               short numOwners = buf.readUnsignedByte();
               segmentOwners[i] = new SocketAddress[numOwners];
               for (int j = 0; j < numOwners; j++) {
                  int memberIndex = ByteBufUtil.readVInt(buf);
                  segmentOwners[i][j] = addresses[memberIndex];
               }
            }
         }
      } else {
         hashFunctionVersion = -1;
         segmentOwners = null;
      }

      channelFactory.receiveTopology(params.cacheName, params.topologyAge, newTopologyId, addresses, segmentOwners,
            hashFunctionVersion);
   }

   private InetSocketAddress[] readTopology(ByteBuf buf) {
      int clusterSize = ByteBufUtil.readVInt(buf);
      InetSocketAddress[] addresses = new InetSocketAddress[clusterSize];
      for (int i = 0; i < clusterSize; i++) {
         String host = ByteBufUtil.readString(buf);
         int port = buf.readUnsignedShort();
         addresses[i] = InetSocketAddress.createUnresolved(host, port);
      }
      return addresses;
   }

   @Override
   public void writeIteratorStartOperation(ByteBuf buf, IntSet segments, String filterConverterFactory,
         int batchSize, boolean metadata, byte[][] filterParameters) {
      if (segments == null) {
         ByteBufUtil.writeSignedVInt(buf, -1);
      } else {
         // TODO use a more compact BitSet implementation, like http://roaringbitmap.org/
         BitSet bitSet = new BitSet();
         segments.forEach((IntConsumer) bitSet::set);
         ByteBufUtil.writeOptionalArray(buf, bitSet.toByteArray());
      }
      ByteBufUtil.writeOptionalString(buf, filterConverterFactory);
      if (filterConverterFactory != null) {
         if (filterParameters != null && filterParameters.length > 0) {
            buf.writeByte(filterParameters.length);
            Arrays.stream(filterParameters).forEach(param -> ByteBufUtil.writeArray(buf, param));
         } else {
            buf.writeByte(0);
         }
      }
      ByteBufUtil.writeVInt(buf, batchSize);
      buf.writeByte(metadata ? 1 : 0);
   }

   @Override
   public int readProjectionSize(ByteBuf buf) {
      return ByteBufUtil.readVInt(buf);
   }

   @Override
   public short readMeta(ByteBuf buf) {
      return buf.readUnsignedByte();
   }

   @Override
   public boolean allowOperationsAndEvents() {
      return true;
   }

   @Override
   public MediaType readKeyType(ByteBuf buf) {
      return CodecUtils.readMediaType(buf);
   }

   @Override
   public MediaType readValueType(ByteBuf buf) {
      return CodecUtils.readMediaType(buf);
   }
}
