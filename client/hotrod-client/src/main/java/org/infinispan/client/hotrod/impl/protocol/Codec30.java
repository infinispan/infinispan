package org.infinispan.client.hotrod.impl.protocol;

import static org.infinispan.client.hotrod.impl.TimeUnitParam.encodeTimeUnits;
import static org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil.limitedHexDump;
import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.lang.annotation.Annotation;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.IntConsumer;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryExpired;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
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
import org.infinispan.client.hotrod.impl.MetadataValueImpl;
import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.MediaTypeIds;
import org.infinispan.commons.util.IntSet;
import org.infinispan.counter.api.CounterState;

import io.netty.buffer.ByteBuf;

/**
 * @since 10.0
 */
public class Codec30 implements Codec {
   static final Log log = LogFactory.getLog(Codec.class);

   public static final String EMPTY_VALUE_CONVERTER = "org.infinispan.server.hotrod.HotRodServer$ToEmptyBytesKeyValueFilterConverter";
   @Override
   public void writeBloomFilter(ByteBuf buf, int bloomFilterBits) {
      if (bloomFilterBits > 0) {
         throw new UnsupportedOperationException("Bloom Filter optimization is not available for versions before 3.1");
      }
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
   public void writeHeader(ByteBuf buf, long messageId, ClientTopology clientTopology, HotRodOperation<?> operation) {
      writeHeader(buf, messageId, clientTopology, operation, HotRodConstants.VERSION_30);
   }

   protected void writeHeader(ByteBuf buf, long messageId, ClientTopology clientTopology, HotRodOperation<?> operation, byte version) {
      buf.writeByte(HotRodConstants.REQUEST_MAGIC);
      ByteBufUtil.writeVLong(buf, messageId);
      buf.writeByte(version);
      buf.writeByte(operation.requestOpCode());
      ByteBufUtil.writeArray(buf, operation.getCacheNameBytes());
      ByteBufUtil.writeVInt(buf, operation.flags());
      buf.writeByte(clientTopology.getClientIntelligence().getValue());
      ByteBufUtil.writeVInt(buf, clientTopology.getTopologyId());
      writeDataTypes(buf, operation.getDataFormat());

      if (log.isTraceEnabled())
         log.tracef("[%s] Wrote header for messageId=%d. Operation code: %#04x(%s). Flags: %#x. Topology id: %s",
               operation.getCacheName(), messageId, operation.requestOpCode(),
               HotRodConstants.Names.of(operation.requestOpCode()), operation.flags(), clientTopology.getTopologyId());
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
   public void writeMultimapSupportDuplicates(ByteBuf buf, boolean supportsDuplicates) {
   }

   @Override
   public Object returnPossiblePrevValue(ByteBuf buf, short status, CacheUnmarshaller unmarshaller) {
      if (HotRodConstants.hasPrevious(status)) {
         return unmarshaller.readValue(buf);
      } else {
         return null;
      }
   }

   @Override
   public <V> MetadataValue<V> returnMetadataValue(ByteBuf buf, short status, CacheUnmarshaller unmarshaller) {
      if (!HotRodConstants.hasPrevious(status)) return null;

      V value = unmarshaller.readValue(buf);
      return new MetadataValueImpl<>(-1, -1, -1, -1, 0, value);
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

   @Override
   public void checkForErrorsInResponseStatus(ByteBuf buf, String cacheName, long messageId, short status, SocketAddress serverAddress) {
      if (log.isTraceEnabled()) log.tracef("[%s] Received operation status: %#x", cacheName, status);

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
               throw new HotRodClientException(msgFromServer, messageId, status);
            }
            case HotRodConstants.ILLEGAL_LIFECYCLE_STATE -> {
               msgFromServer = ByteBufUtil.readString(buf);
               throw new RemoteIllegalLifecycleStateException(msgFromServer, messageId, status, serverAddress);
            }
            case HotRodConstants.NODE_SUSPECTED -> {
               // Handle both Infinispan's and JGroups' suspicions
               msgFromServer = ByteBufUtil.readString(buf);
               if (log.isTraceEnabled())
                  log.tracef("[%s] A remote node was suspected while executing messageId=%d. " +
                              "Check if retry possible. Message from server: %s",
                        cacheName, messageId, msgFromServer);
               throw new RemoteNodeSuspectException(msgFromServer, messageId, status);
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
   public AbstractClientEvent readCacheEvent(ByteBuf buf, long messageId, Function<byte[], DataFormat> listenerDataFormat, short eventTypeId, ClassAllowList allowList, SocketAddress serverAddress) {
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
            checkForErrorsInResponseStatus(buf, null, messageId, status, serverAddress);
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
         if (!parameters.isEmpty()) {
            parameters.forEach((key, value) -> {
               ByteBufUtil.writeString(buf, key);
               ByteBufUtil.writeString(buf, value);
            });
         }
      }
   }
}
