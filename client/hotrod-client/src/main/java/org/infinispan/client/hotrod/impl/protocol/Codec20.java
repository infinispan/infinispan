package org.infinispan.client.hotrod.impl.protocol;

import static org.infinispan.client.hotrod.impl.Util.await;
import static org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil.hexDump;

import java.lang.annotation.Annotation;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.counter.impl.HotRodCounterEvent;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.event.impl.AbstractClientEvent;
import org.infinispan.client.hotrod.event.impl.CreatedEventImpl;
import org.infinispan.client.hotrod.event.impl.CustomEventImpl;
import org.infinispan.client.hotrod.event.impl.ModifiedEventImpl;
import org.infinispan.client.hotrod.event.impl.RemovedEventImpl;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.exceptions.RemoteIllegalLifecycleStateException;
import org.infinispan.client.hotrod.exceptions.RemoteNodeSuspectException;
import org.infinispan.client.hotrod.impl.operations.BulkGetKeysOperation;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.counter.api.CounterState;

import io.netty.buffer.ByteBuf;

/**
 * A Hot Rod encoder/decoder for version 2.0 of the protocol.
 *
 * @author Galder Zamarreño
 * @since 7.0
 */
public class Codec20 implements Codec, HotRodConstants {

   private static final Log log = LogFactory.getLog(Codec20.class, Log.class);

   final boolean trace = getLog().isTraceEnabled();

   @Override
   public <T> T readUnmarshallByteArray(ByteBuf buf, short status, ClassWhiteList whitelist, Marshaller marshaller) {
      return CodecUtils.readUnmarshallByteArray(buf, status, whitelist, marshaller);
   }

   public void writeClientListenerInterests(ByteBuf buf, Set<Class<? extends Annotation>> classes) {
      // No-op
   }

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_20);
   }

   @Override
   public void writeClientListenerParams(ByteBuf buf, ClientListener clientListener,
                                         byte[][] filterFactoryParams, byte[][] converterFactoryParams) {
      buf.writeByte((short) (clientListener.includeCurrentState() ? 1 : 0));
      writeNamedFactory(buf, clientListener.filterFactoryName(), filterFactoryParams);
      writeNamedFactory(buf, clientListener.converterFactoryName(), converterFactoryParams);
   }

   @Override
   public void writeExpirationParams(ByteBuf buf, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      if (!CodecUtils.isIntCompatible(lifespan)) {
         log.warn("Lifespan value greater than the max supported size (Integer.MAX_VALUE), this can cause precision loss");
      }
      if (!CodecUtils.isIntCompatible(maxIdle)) {
         log.warn("MaxIdle value greater than the max supported size (Integer.MAX_VALUE), this can cause precision loss");
      }
      int lifespanSeconds = CodecUtils.toSeconds(lifespan, lifespanTimeUnit);
      int maxIdleSeconds = CodecUtils.toSeconds(maxIdle, maxIdleTimeUnit);
      ByteBufUtil.writeVInt(buf, lifespanSeconds);
      ByteBufUtil.writeVInt(buf, maxIdleSeconds);
   }

   @Override
   public int estimateExpirationSize(long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      int lifespanSeconds = CodecUtils.toSeconds(lifespan, lifespanTimeUnit);
      int maxIdleSeconds = CodecUtils.toSeconds(maxIdle, maxIdleTimeUnit);
      return ByteBufUtil.estimateVIntSize(lifespanSeconds) + ByteBufUtil.estimateVIntSize(maxIdleSeconds);
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

   protected HeaderParams writeHeader(
         ByteBuf buf, HeaderParams params, byte version) {
      buf.writeByte(HotRodConstants.REQUEST_MAGIC);
      ByteBufUtil.writeVLong(buf, params.messageId);
      buf.writeByte(version);
      buf.writeByte(params.opCode);
      ByteBufUtil.writeArray(buf, params.cacheName);
      int joinedFlags = params.flags;
      ByteBufUtil.writeVInt(buf, joinedFlags);
      buf.writeByte(params.clientIntel);
      int topologyId = params.topologyId.get();
      ByteBufUtil.writeVInt(buf, topologyId);

      if (trace)
         getLog().tracef("[%s] Wrote header for messageId=%d to %s. Operation code: %#04x(%s). Flags: %#x. Topology id: %s",
               new String(params.cacheName), params.messageId, buf, params.opCode,
               Names.of(params.opCode), joinedFlags, topologyId);

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
         final Log localLog = getLog();
         localLog.invalidMagicNumber(HotRodConstants.RESPONSE_MAGIC, magic);
         if (trace)
            localLog.tracef("Socket dump: %s", hexDump(buf));

         throw new InvalidResponseException(String.format("Invalid magic number. Expected %#x and received %#x", HotRodConstants.RESPONSE_MAGIC, magic));
      }
      long receivedMessageId = ByteBufUtil.readVLong(buf);
      if (trace) {
         getLog().tracef("Received response for messageId=%d", receivedMessageId);
      }
      return receivedMessageId;
   }

   @Override
   public short readOpCode(ByteBuf buf) {
      short receivedOpCode = buf.readUnsignedByte();
      if (trace)
         getLog().tracef("Received operation code is: %#04x(%s)", receivedOpCode, Names.of(receivedOpCode));
      return receivedOpCode;
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
         throw new InvalidResponseException(String.format(
               "[%s] Invalid response operation. Expected %#x and received %#x",
               new String(params.cacheName), params.opRespCode, receivedOpCode));
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
   public <K> CloseableIterator<K> keyIterator(RemoteCache<K, ?> remoteCache, OperationsFactory operationsFactory, int batchSize) {
      BulkGetKeysOperation<K> op = operationsFactory.newBulkGetKeysOperation(0);
      Set<K> keys = await(op.execute());
      return Closeables.iterator(keys.iterator());
   }

   @Override
   public AbstractClientEvent readCacheEvent(ByteBuf buf, Function<byte[], DataFormat> listenerDataFormat, short eventTypeId, ClassWhiteList whitelist, SocketAddress serverAddress) {
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
         case ERROR_RESPONSE:
            checkForErrorsInResponseStatus(buf, null, status, serverAddress);
            // Fall through if we didn't throw an exception already
         default:
            throw log.unknownEvent(eventTypeId);
      }

      byte[] listenerId = ByteBufUtil.readArray(buf);

      short isCustom = buf.readUnsignedByte();
      boolean isRetried = buf.readUnsignedByte() == 1;
      DataFormat dataFormat = listenerDataFormat.apply(listenerId);
      if (isCustom == 1) {
         final Object eventData = dataFormat.valueToObj(ByteBufUtil.readArray(buf), status, whitelist);
         return createCustomEvent(listenerId, eventData, eventType, isRetried);
      } else {
         switch (eventType) {
            case CLIENT_CACHE_ENTRY_CREATED:
               long createdDataVersion = buf.readLong();
               return createCreatedEvent(listenerId, dataFormat.keyToObj(ByteBufUtil.readArray(buf), status, whitelist), createdDataVersion, isRetried);
            case CLIENT_CACHE_ENTRY_MODIFIED:
               long modifiedDataVersion = buf.readLong();
               return createModifiedEvent(listenerId, dataFormat.keyToObj(ByteBufUtil.readArray(buf), status, whitelist), modifiedDataVersion, isRetried);
            case CLIENT_CACHE_ENTRY_REMOVED:
               return createRemovedEvent(listenerId, dataFormat.keyToObj(ByteBufUtil.readArray(buf), status, whitelist), isRetried);
            default:
               throw log.unknownEvent(eventTypeId);
         }
      }
   }

   @Override
   public Object returnPossiblePrevValue(ByteBuf buf, short status, int flags, ClassWhiteList whitelist, Marshaller marshaller) {
      if (HotRodConstants.hasPrevious(status)) {
         return CodecUtils.readUnmarshallByteArray(buf, status, whitelist, marshaller);
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

   @Override
   public Log getLog() {
      return log;
   }

   protected void checkForErrorsInResponseStatus(ByteBuf buf, HeaderParams params, short status, SocketAddress serverAddress) {
      final Log localLog = getLog();
      if (trace) localLog.tracef("[%s] Received operation status: %#x", new String(params.cacheName), status);

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
               if (status == HotRodConstants.COMMAND_TIMEOUT_STATUS && trace) {
                  localLog.tracef("Server-side timeout performing operation: %s", msgFromServer);
               } else {
                  localLog.errorFromServer(msgFromServer);
               }
               throw new HotRodClientException(msgFromServer, params.messageId, status);
            }
            case HotRodConstants.ILLEGAL_LIFECYCLE_STATE:
               msgFromServer = ByteBufUtil.readString(buf);
               throw new RemoteIllegalLifecycleStateException(msgFromServer, params.messageId, status, serverAddress);
            case HotRodConstants.NODE_SUSPECTED:
               // Handle both Infinispan's and JGroups' suspicions
               msgFromServer = ByteBufUtil.readString(buf);
               if (trace)
                  localLog.tracef("[%s] A remote node was suspected while executing messageId=%d. " +
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
      final Log localLog = getLog();
      int newTopologyId = ByteBufUtil.readVInt(buf);

      SocketAddress[] addresses = readTopology(buf);

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

      int currentTopology = channelFactory.getTopologyId(params.cacheName);
      int topologyAge = channelFactory.getTopologyAge();
      // Since the header is now created only once (not during each retry) the topologyAge in header may be non-actual
      // but we should still accept the topology
      if (params.topologyAge < topologyAge || params.topologyAge == topologyAge && currentTopology != newTopologyId) {
         params.topologyId.set(newTopologyId);
         List<SocketAddress> addressList = Arrays.asList(addresses);
         if (localLog.isInfoEnabled()) {
            localLog.newTopology(newTopologyId, topologyAge,
                  addresses.length, new HashSet<>(addressList));
         }
         channelFactory.updateServers(addressList, params.cacheName, false);
         if (hashFunctionVersion >= 0) {
            if (trace) {
               String cacheNameString = new String(params.cacheName);
               if (hashFunctionVersion == 0)
                  localLog.tracef("[%s] Not using a consistent hash function (hash function version == 0).", cacheNameString);
               else
                  localLog.tracef("[%s] Updating client hash function with %s number of segments", cacheNameString, segmentOwners.length);
            }
            channelFactory.updateHashFunction(segmentOwners,
                  segmentOwners.length, hashFunctionVersion, params.cacheName, params.topologyId);
         }
      } else {
         if (trace)
            localLog.tracef("[%s] Outdated topology received (topology id = %s, topology age = %s), so ignoring it: %s",
                  new String(params.cacheName), newTopologyId, topologyAge, Arrays.toString(addresses));
      }
   }

   private SocketAddress[] readTopology(ByteBuf buf) {
      int clusterSize = ByteBufUtil.readVInt(buf);
      SocketAddress[] addresses = new SocketAddress[clusterSize];
      for (int i = 0; i < clusterSize; i++) {
         String host = ByteBufUtil.readString(buf);
         int port = buf.readUnsignedShort();
         addresses[i] = InetSocketAddress.createUnresolved(host, port);
      }
      return addresses;
   }

}
