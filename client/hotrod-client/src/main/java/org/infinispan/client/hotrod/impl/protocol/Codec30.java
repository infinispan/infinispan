package org.infinispan.client.hotrod.impl.protocol;

import static org.infinispan.client.hotrod.impl.TimeUnitParam.encodeTimeUnits;
import static org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil.limitedHexDump;
import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.lang.annotation.Annotation;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryExpired;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.counter.impl.HotRodCounterEvent;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.event.impl.AbstractClientEvent;
import org.infinispan.client.hotrod.event.impl.CreatedEventImpl;
import org.infinispan.client.hotrod.event.impl.CustomEventImpl;
import org.infinispan.client.hotrod.event.impl.ExpiredEventImpl;
import org.infinispan.client.hotrod.event.impl.ModifiedEventImpl;
import org.infinispan.client.hotrod.event.impl.RemovedEventImpl;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.RemoteIllegalLifecycleStateException;
import org.infinispan.client.hotrod.exceptions.RemoteNodeSuspectException;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.operations.PingResponse;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelInitializer;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelPool;
import org.infinispan.client.hotrod.impl.transport.netty.V5ChannelPool;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.MediaTypeIds;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.counter.api.CounterState;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.EventExecutor;

/**
 * @since 10.0
 */
public class Codec30 implements Codec {
   static final Log log = LogFactory.getLog(Codec.class, Log.class);

   public static final String EMPTY_VALUE_CONVERTER = "org.infinispan.server.hotrod.HotRodServer$ToEmptyBytesKeyValueFilterConverter";
   @Override
   public void writeBloomFilter(ByteBuf buf, int bloomFilterBits) {
      if (bloomFilterBits > 0) {
         throw new UnsupportedOperationException("Bloom Filter optimization is not available for versions before 3.1");
      }
   }

   @Override
   public int estimateHeaderSize(HeaderParams params) {
      return 1 + ByteBufUtil.estimateVLongSize(params.messageId) + 1 + 1 +
            ByteBufUtil.estimateArraySize(params.cacheName) + ByteBufUtil.estimateVIntSize(params.flags) +
            1 + 1 + ByteBufUtil.estimateVIntSize(params.getClientTopology().get().getTopologyId());
   }

   @Override
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

   private static CounterState decodeOldState(short encoded) {
      return switch (encoded & 0x03) {
         case 0 -> CounterState.VALID;
         case 0x01 -> CounterState.LOWER_BOUND_REACHED;
         case 0x02 -> CounterState.UPPER_BOUND_REACHED;
         default -> throw new IllegalStateException();
      };
   }

   private static CounterState decodeNewState(short encoded) {
      return switch (encoded & 0x0C) {
         case 0 -> CounterState.VALID;
         case 0x04 -> CounterState.LOWER_BOUND_REACHED;
         case 0x08 -> CounterState.UPPER_BOUND_REACHED;
         default -> throw new IllegalStateException();
      };
   }

   @Override
   public int estimateSizeMultimapSupportsDuplicated() {
      return 0;
   }

   @Override
   public void writeMultimapSupportDuplicates(ByteBuf buf, boolean supportsDuplicates) {
   }

   @Override
   public Object returnPossiblePrevValue(ByteBuf buf, short status, DataFormat dataFormat, ClassAllowList allowList, Marshaller marshaller) {
      if (HotRodConstants.hasPrevious(status)) {
         return dataFormat.valueToObj(ByteBufUtil.readArray(buf), allowList);
      } else {
         return null;
      }
   }

   protected AbstractClientEvent createRemovedEvent(byte[] listenerId, Object key, boolean isRetried) {
      return new RemovedEventImpl<>(listenerId, key, isRetried);
   }

   protected AbstractClientEvent createModifiedEvent(byte[] listenerId, Object key, long dataVersion, boolean isRetried) {
      return new ModifiedEventImpl<>(listenerId, key, dataVersion, isRetried);
   }

   protected AbstractClientEvent createCreatedEvent(byte[] listenerId, Object key, long dataVersion, boolean isRetried) {
      return new CreatedEventImpl<>(listenerId, key, dataVersion, isRetried);
   }

   protected AbstractClientEvent createCustomEvent(byte[] listenerId, Object eventData, ClientEvent.Type eventType, boolean isRetried) {
      return new CustomEventImpl<>(listenerId, eventData, isRetried, eventType);
   }

   protected void checkForErrorsInResponseStatus(ByteBuf buf, HeaderParams params, short status, SocketAddress serverAddress) {
      if (log.isTraceEnabled()) log.tracef("[%s] Received operation status: %#x", new String(params.cacheName), status);

      String msgFromServer;
      try {
         switch (status) {
            case HotRodConstants.INVALID_MAGIC_OR_MESSAGE_ID_STATUS, HotRodConstants.REQUEST_PARSING_ERROR_STATUS,
                  HotRodConstants.UNKNOWN_COMMAND_STATUS, HotRodConstants.SERVER_ERROR_STATUS,
                  HotRodConstants.COMMAND_TIMEOUT_STATUS, HotRodConstants.UNKNOWN_VERSION_STATUS -> {
               // If error, the body of the message just contains a message
               msgFromServer = ByteBufUtil.readString(buf);
               if (status == HotRodConstants.COMMAND_TIMEOUT_STATUS && log.isTraceEnabled()) {
                  log.tracef("Server-side timeout performing operation: %s", msgFromServer);
               } else {
                  HOTROD.errorFromServer(msgFromServer);
               }
               throw new HotRodClientException(msgFromServer, params.messageId, status);
            }
            case HotRodConstants.ILLEGAL_LIFECYCLE_STATE -> {
               msgFromServer = ByteBufUtil.readString(buf);
               throw new RemoteIllegalLifecycleStateException(msgFromServer, params.messageId, status, serverAddress);
            }
            case HotRodConstants.NODE_SUSPECTED -> {
               // Handle both Infinispan's and JGroups' suspicions
               msgFromServer = ByteBufUtil.readString(buf);
               if (log.isTraceEnabled())
                  log.tracef("[%s] A remote node was suspected while executing messageId=%d. " +
                              "Check if retry possible. Message from server: %s",
                        new String(params.cacheName), params.messageId, msgFromServer);
               throw new RemoteNodeSuspectException(msgFromServer, params.messageId, status);
            }
            default -> {
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
               // TODO: why did Codec20 not having anything here????
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
      if (params.clientIntelligence == ClientIntelligence.HASH_DISTRIBUTION_AWARE.getValue()) {
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
   public ChannelPool createPool(EventExecutor executor, SocketAddress address, ChannelInitializer channelInitializer, BiConsumer<ChannelPool, ChannelFactory.ChannelEventType> connectionFailureListener, Configuration configuration) {
      return V5ChannelPool.createAndStartPool(address, channelInitializer, connectionFailureListener);
   }

   @Override
   public void writeClientListenerParams(ByteBuf buf, ClientListener clientListener, byte[][] filterFactoryParams, byte[][] converterFactoryParams) {
      buf.writeByte((short) (clientListener.includeCurrentState() ? 1 : 0));
      writeNamedFactory(buf, clientListener.filterFactoryName(), filterFactoryParams);
      writeNamedFactory(buf, clientListener.converterFactoryName(), converterFactoryParams);
      buf.writeByte((short) (clientListener.useRawData() ? 1 : 0));
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

   @Override
   public AbstractClientEvent readCacheEvent(ByteBuf buf, Function<byte[], DataFormat> listenerDataFormat, short eventTypeId, ClassAllowList allowList, SocketAddress serverAddress) {
      short status = buf.readUnsignedByte();
      buf.readUnsignedByte(); // ignore, no topology expected
      ClientEvent.Type eventType;
      switch (eventTypeId) {
         case HotRodConstants.CACHE_ENTRY_CREATED_EVENT_RESPONSE:
            eventType = ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED;
            break;
         case HotRodConstants.CACHE_ENTRY_MODIFIED_EVENT_RESPONSE:
            eventType = ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED;
            break;
         case HotRodConstants.CACHE_ENTRY_REMOVED_EVENT_RESPONSE:
            eventType = ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED;
            break;
         case HotRodConstants.CACHE_ENTRY_EXPIRED_EVENT_RESPONSE:
            eventType = ClientEvent.Type.CLIENT_CACHE_ENTRY_EXPIRED;
            break;
         case HotRodConstants.ERROR_RESPONSE:
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
            case CLIENT_CACHE_ENTRY_CREATED -> {
               Object createdKey = dataFormat.keyToObj(ByteBufUtil.readArray(buf), allowList);
               long createdDataVersion = buf.readLong();
               return createCreatedEvent(listenerId, createdKey, createdDataVersion, isRetried);
            }
            case CLIENT_CACHE_ENTRY_MODIFIED -> {
               Object modifiedKey = dataFormat.keyToObj(ByteBufUtil.readArray(buf), allowList);
               long modifiedDataVersion = buf.readLong();
               return createModifiedEvent(listenerId, modifiedKey, modifiedDataVersion, isRetried);
            }
            case CLIENT_CACHE_ENTRY_REMOVED -> {
               Object removedKey = dataFormat.keyToObj(ByteBufUtil.readArray(buf), allowList);
               return createRemovedEvent(listenerId, removedKey, isRetried);
            }
            case CLIENT_CACHE_ENTRY_EXPIRED -> {
               Object expiredKey = dataFormat.keyToObj(ByteBufUtil.readArray(buf), allowList);
               return createExpiredEvent(listenerId, expiredKey);
            }
            default -> throw HOTROD.unknownEvent(eventTypeId);
         }
      }
   }

   protected AbstractClientEvent createExpiredEvent(byte[] listenerId, Object key) {
      return new ExpiredEventImpl<>(listenerId, key);
   }

   @Override
   public void writeExpirationParams(ByteBuf buf, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      byte timeUnits = encodeTimeUnits(lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      buf.writeByte(timeUnits);
      if (lifespan > 0) {
         ByteBufUtil.writeVLong(buf, lifespan);
      }
      if (maxIdle > 0) {
         ByteBufUtil.writeVLong(buf, maxIdle);
      }
   }

   @Override
   public int estimateExpirationSize(long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      return 1 + (lifespan > 0 ? ByteBufUtil.estimateVLongSize(lifespan) : 0) + (lifespan > 0 ? ByteBufUtil.estimateVLongSize(maxIdle) : 0);
   }

   @Override
   public <K, V> CloseableIterator<Map.Entry<K, V>> entryIterator(RemoteCache<K, V> remoteCache, IntSet segments, int batchSize) {
      return castEntryIterator(remoteCache.retrieveEntries(null, segments, batchSize));
   }

   protected <K, V> CloseableIterator<Map.Entry<K, V>> castEntryIterator(CloseableIterator iterator) {
      return iterator;
   }

   @Override
   public void writeIteratorStartOperation(ByteBuf buf, IntSet segments, String filterConverterFactory, int batchSize, boolean metadata, byte[][] filterParameters) {
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
   public void writeClientListenerInterests(ByteBuf buf, Set<Class<? extends Annotation>> classes) {
      byte listenerInterests = 0;
      if (classes.contains(ClientCacheEntryCreated.class))
         listenerInterests = (byte) (listenerInterests | 0x01);
      if (classes.contains(ClientCacheEntryModified.class))
         listenerInterests = (byte) (listenerInterests | 0x02);
      if (classes.contains(ClientCacheEntryRemoved.class))
         listenerInterests = (byte) (listenerInterests | 0x04);
      if (classes.contains(ClientCacheEntryExpired.class))
         listenerInterests = (byte) (listenerInterests | 0x08);

      ByteBufUtil.writeVInt(buf, listenerInterests);
   }

   @Override
   public <K> CloseableIterator<K> keyIterator(RemoteCache<K, ?> remoteCache, OperationsFactory operationsFactory, IntSet segments, int batchSize) {
      return new IteratorMapper<>(remoteCache.retrieveEntries(
            // Use the ToEmptyBytesKeyValueFilterConverter to remove value payload
            EMPTY_VALUE_CONVERTER, segments, batchSize), e -> (K) e.getKey());
   }

   protected HeaderParams writeHeader(ByteBuf buf, HeaderParams params, byte version) {
      ClientTopology clientTopology = params.clientTopology.get();
      buf.writeByte(HotRodConstants.REQUEST_MAGIC);
      ByteBufUtil.writeVLong(buf, params.messageId);
      buf.writeByte(version);
      buf.writeByte(params.opCode);
      ByteBufUtil.writeArray(buf, params.cacheName);
      int joinedFlags = params.flags;
      ByteBufUtil.writeVInt(buf, joinedFlags);
      byte clientIntel = clientTopology.getClientIntelligence().getValue();
      // set the client intelligence byte sent to read the response
      params.clientIntelligence = clientIntel;
      buf.writeByte(clientIntel);
      int topologyId = clientTopology.getTopologyId();
      ByteBufUtil.writeVInt(buf, topologyId);

      if (log.isTraceEnabled())
         log.tracef("[%s] Wrote header for messageId=%d. Operation code: %#04x(%s). Flags: %#x. Topology id: %s",
               new String(params.cacheName), params.messageId, params.opCode,
               HotRodConstants.Names.of(params.opCode), joinedFlags, topologyId);

      writeDataTypes(buf, params.dataFormat);
      return params;
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

   @Override
   public boolean isObjectStorageHinted(PingResponse pingResponse) {
      return pingResponse.isObjectStorage();
   }

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_30);
   }
}
