package org.infinispan.client.hotrod.impl.protocol;

import static org.infinispan.commons.util.Util.hexDump;
import static org.infinispan.commons.util.Util.printArray;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.client.hotrod.VersionedMetadata;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryModifiedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.exceptions.RemoteIllegalLifecycleStateException;
import org.infinispan.client.hotrod.exceptions.RemoteNodeSuspectException;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Either;

/**
 * A Hot Rod encoder/decoder for version 2.0 of the protocol.
 *
 * @author Galder Zamarreño
 * @since 7.0
 */
public class Codec20 implements Codec, HotRodConstants {

   private static final Log log = LogFactory.getLog(Codec20.class, Log.class);

   static final AtomicLong MSG_ID = new AtomicLong();

   final boolean trace = getLog().isTraceEnabled();

   @Override
   public <T> T readUnmarshallByteArray(Transport transport, short status, List<String> whitelist) {
      return CodecUtils.readUnmarshallByteArray(transport, status, whitelist);
   }

   @Override
   public <T extends InputStream & VersionedMetadata> T readAsStream(Transport transport, VersionedMetadata versionedMetadata, Runnable afterClose) {
      return (T)new TransportInputStream(transport, versionedMetadata, afterClose);
   }

   @Override
   public OutputStream writeAsStream(Transport transport, Runnable afterClose) {
      return new TransportOutputStream(transport, afterClose);
   }

   public void writeClientListenerInterests(Transport transport, Set<Class<? extends Annotation>> classes) {
      // No-op
   }

   @Override
   public HeaderParams writeHeader(Transport transport, HeaderParams params) {
      return writeHeader(transport, params, HotRodConstants.VERSION_20);
   }

   @Override
   public void writeClientListenerParams(Transport transport, ClientListener clientListener,
         byte[][] filterFactoryParams, byte[][] converterFactoryParams) {
      transport.writeByte((short)(clientListener.includeCurrentState() ? 1 : 0));
      writeNamedFactory(transport, clientListener.filterFactoryName(), filterFactoryParams);
      writeNamedFactory(transport, clientListener.converterFactoryName(), converterFactoryParams);
   }

   @Override
   public void writeExpirationParams(Transport transport, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      if (!CodecUtils.isIntCompatible(lifespan)) {
         log.warn("Lifespan value greater than the max supported size (Integer.MAX_VALUE), this can cause precision loss");
      }
      if (!CodecUtils.isIntCompatible(maxIdle)) {
         log.warn("MaxIdle value greater than the max supported size (Integer.MAX_VALUE), this can cause precision loss");
      }
      int lifespanSeconds = CodecUtils.toSeconds(lifespan, lifespanTimeUnit);
      int maxIdleSeconds = CodecUtils.toSeconds(maxIdle, maxIdleTimeUnit);
      transport.writeVInt(lifespanSeconds);
      transport.writeVInt(maxIdleSeconds);
   }


   private void writeNamedFactory(Transport transport, String factoryName, byte[][] params) {
      transport.writeString(factoryName);
      if (!factoryName.isEmpty()) {
         // A named factory was written, how many parameters?
         if (params != null) {
            transport.writeByte((short) params.length);
            for (byte[] param : params)
               transport.writeArray(param);
         } else {
            transport.writeByte((short) 0);
         }
      }
   }

   protected HeaderParams writeHeader(
         Transport transport, HeaderParams params, byte version) {
      transport.writeByte(HotRodConstants.REQUEST_MAGIC);
      transport.writeVLong(params.messageId(MSG_ID.incrementAndGet()).messageId);
      transport.writeByte(version);
      transport.writeByte(params.opCode);
      transport.writeArray(params.cacheName);
      int joinedFlags = params.flags;
      transport.writeVInt(joinedFlags);
      transport.writeByte(params.clientIntel);
      int topologyId = params.topologyId.get();
      transport.writeVInt(topologyId);

      if (trace)
         getLog().tracef("Wrote header for messageId=%d to %s. Operation code: %#04x. Flags: %#x. Topology id: %s",
            params.messageId, transport, params.opCode, joinedFlags, topologyId);

      return params;
   }

   @Override
   public short readHeader(Transport transport, HeaderParams params) {
      short magic = readMagic(transport);
      long receivedMessageId = readMessageId(transport, params);
      short receivedOpCode = transport.readByte();
      return readPartialHeader(transport, params, receivedOpCode);
   }

   private short readPartialHeader(Transport transport, HeaderParams params, short receivedOpCode) {
      // Read both the status and new topology (if present),
      // before deciding how to react to error situations.
      short status = transport.readByte();
      readNewTopologyIfPresent(transport, params);

      // Now that all headers values have been read, check the error responses.
      // This avoids situatations where an exceptional return ends up with
      // the socket containing data from previous request responses.
      if (receivedOpCode != params.opRespCode) {
         if (receivedOpCode == HotRodConstants.ERROR_RESPONSE) {
            checkForErrorsInResponseStatus(transport, params, status);
         }
         throw new InvalidResponseException(String.format(
               "Invalid response operation. Expected %#x and received %#x",
               params.opRespCode, receivedOpCode));
      }

      if (trace)
         getLog().tracef("Received operation code is: %#04x", receivedOpCode);

      return status;
   }

   @Override
   public ClientEvent readEvent(Transport transport, byte[] expectedListenerId, Marshaller marshaller, List<String> whitelist) {
      readMagic(transport);
      readMessageId(transport, null);
      short eventTypeId = transport.readByte();
      return readPartialEvent(transport, expectedListenerId, marshaller, eventTypeId, whitelist);
   }

   protected ClientEvent readPartialEvent(Transport transport, byte[] expectedListenerId, Marshaller marshaller, short eventTypeId, List<String> whitelist) {
      short status = transport.readByte();
      transport.readByte(); // ignore, no topology expected
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
            checkForErrorsInResponseStatus(transport, null, status);
            // Fall through if we didn't throw an exception already
         default:
            throw log.unknownEvent(eventTypeId);
      }

      byte[] listenerId = transport.readArray();
      if (!Arrays.equals(listenerId, expectedListenerId))
         throw log.unexpectedListenerId(printArray(listenerId), printArray(expectedListenerId));

      short isCustom = transport.readByte();
      boolean isRetried = transport.readByte() == 1 ? true : false;

      if (isCustom == 1) {
         final Object eventData = MarshallerUtil.bytes2obj(marshaller, transport.readArray(), status, whitelist);
         return createCustomEvent(eventData, eventType, isRetried);
      } else {
         switch (eventType) {
            case CLIENT_CACHE_ENTRY_CREATED:
               Object createdKey = MarshallerUtil.bytes2obj(marshaller, transport.readArray(), status, whitelist);
               long createdDataVersion = transport.readLong();
               return createCreatedEvent(createdKey, createdDataVersion, isRetried);
            case CLIENT_CACHE_ENTRY_MODIFIED:
               Object modifiedKey = MarshallerUtil.bytes2obj(marshaller, transport.readArray(), status, whitelist);
               long modifiedDataVersion = transport.readLong();
               return createModifiedEvent(modifiedKey, modifiedDataVersion, isRetried);
            case CLIENT_CACHE_ENTRY_REMOVED:
               Object removedKey = MarshallerUtil.bytes2obj(marshaller, transport.readArray(), status, whitelist);
               return createRemovedEvent(removedKey, isRetried);
            default:
               throw log.unknownEvent(eventTypeId);
         }
      }
   }

   @Override
   public Either<Short, ClientEvent> readHeaderOrEvent(Transport transport, HeaderParams params, byte[] expectedListenerId, Marshaller marshaller, List<String> whitelist) {
      readMagic(transport);
      readMessageId(transport, null);
      short opCode = transport.readByte();
      switch (opCode) {
         case CACHE_ENTRY_CREATED_EVENT_RESPONSE:
         case CACHE_ENTRY_MODIFIED_EVENT_RESPONSE:
         case CACHE_ENTRY_REMOVED_EVENT_RESPONSE:
            ClientEvent clientEvent = readPartialEvent(transport, expectedListenerId, marshaller, opCode, whitelist);
            return Either.newRight(clientEvent);
         default:
            return Either.newLeft(readPartialHeader(transport, params, opCode));
      }
   }

   @Override
   public Object returnPossiblePrevValue(Transport transport, short status, int flags, List<String> whitelist) {
      Marshaller marshaller = transport.getTransportFactory().getMarshaller();
      if (HotRodConstants.hasPrevious(status)) {
         byte[] bytes = transport.readArray();
         if (trace) getLog().tracef("Previous value bytes is: %s", printArray(bytes, false));
         //0-length response means null
         return bytes.length == 0 ? null : MarshallerUtil.bytes2obj(marshaller, bytes, status, whitelist);
      } else {
         return null;
      }
   }

   protected ClientEvent createRemovedEvent(final Object key, final boolean isRetried) {
      return new ClientCacheEntryRemovedEvent() {
         @Override public Object getKey() { return key; }
         @Override public Type getType() { return Type.CLIENT_CACHE_ENTRY_REMOVED; }
         @Override public boolean isCommandRetried() { return isRetried; }
         @Override
         public String toString() {
            return "ClientCacheEntryRemovedEvent(" + "key=" + key + ")";
         }
      };
   }

   protected ClientCacheEntryModifiedEvent createModifiedEvent(final Object key, final long dataVersion, final boolean isRetried) {
      return new ClientCacheEntryModifiedEvent() {
         @Override public Object getKey() { return key; }
         @Override public long getVersion() { return dataVersion; }
         @Override public Type getType() { return Type.CLIENT_CACHE_ENTRY_MODIFIED; }
         @Override public boolean isCommandRetried() { return isRetried; }
         @Override
         public String toString() {
            return "ClientCacheEntryModifiedEvent(" + "key=" + key
                  + ",dataVersion=" + dataVersion + ")";
         }
      };
   }

   protected ClientCacheEntryCreatedEvent<Object> createCreatedEvent(final Object key, final long dataVersion, final boolean isRetried) {
      return new ClientCacheEntryCreatedEvent<Object>() {
         @Override public Object getKey() { return key; }
         @Override public long getVersion() { return dataVersion; }
         @Override public Type getType() { return Type.CLIENT_CACHE_ENTRY_CREATED; }
         @Override public boolean isCommandRetried() { return isRetried; }
         @Override
         public String toString() {
            return "ClientCacheEntryCreatedEvent(" + "key=" + key
                  + ",dataVersion=" + dataVersion + ")";
         }
      };
   }

   protected ClientCacheEntryCustomEvent<Object> createCustomEvent(final Object eventData, final ClientEvent.Type eventType, final boolean isRetried) {
      return new ClientCacheEntryCustomEvent<Object>() {
         @Override public Object getEventData() { return eventData; }
         @Override public Type getType() { return eventType; }
         @Override public boolean isCommandRetried() { return isRetried; }
         @Override
         public String toString() {
            return "ClientCacheEntryCustomEvent(" + "eventData=" + eventData + ", eventType=" + eventType + ")";
         }
      };
   }

   private long readMessageId(Transport transport, HeaderParams params) {
      long receivedMessageId = transport.readVLong();
      final Log localLog = getLog();
      // If received id is 0, it could be that a failure was noted before the
      // message id was detected, so don't consider it to a message id error
      if (params != null && receivedMessageId != params.messageId && receivedMessageId != 0) {
         String message = "Invalid message id. Expected %d and received %d";
         localLog.invalidMessageId(params.messageId, receivedMessageId);
         if (trace)
            localLog.tracef("Socket dump: %s", hexDump(transport.dumpStream()));

         throw new InvalidResponseException(String.format(message, params.messageId, receivedMessageId));
      }

      if (trace)
         localLog.tracef("Received response for messageId=%d", receivedMessageId);

      return receivedMessageId;
   }

   private short readMagic(Transport transport) {
      short magic = transport.readByte();
      if (magic != HotRodConstants.RESPONSE_MAGIC) {
         final Log localLog = getLog();
         String message = "Invalid magic number. Expected %#x and received %#x";
         localLog.invalidMagicNumber(HotRodConstants.RESPONSE_MAGIC, magic);
         if (trace)
            localLog.tracef("Socket dump: %s", hexDump(transport.dumpStream()));

         throw new InvalidResponseException(String.format(message, HotRodConstants.RESPONSE_MAGIC, magic));
      }
      return magic;
   }

   @Override
   public Log getLog() {
      return log;
   }

   protected void checkForErrorsInResponseStatus(Transport transport, HeaderParams params, short status) {
      final Log localLog = getLog();
      if (trace) localLog.tracef("Received operation status: %#x", status);

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
               msgFromServer = transport.readString();
               if (status == HotRodConstants.COMMAND_TIMEOUT_STATUS && trace) {
                  localLog.tracef("Server-side timeout performing operation: %s", msgFromServer);
               } else {
                  localLog.errorFromServer(msgFromServer);
               }
               throw new HotRodClientException(msgFromServer, params.messageId, status);
            }
            case HotRodConstants.ILLEGAL_LIFECYCLE_STATE:
               msgFromServer = transport.readString();
               throw new RemoteIllegalLifecycleStateException(msgFromServer, params.messageId, status, transport.getRemoteSocketAddress());
            case HotRodConstants.NODE_SUSPECTED:
               // Handle both Infinispan's and JGroups' suspicions
               msgFromServer = transport.readString();
               if (trace)
                  localLog.tracef("A remote node was suspected while executing messageId=%d. " +
                        "Check if retry possible. Message from server: %s", params.messageId, msgFromServer);

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
               transport.invalidate();
            }
         }
      }
   }

   protected void readNewTopologyIfPresent(Transport transport, HeaderParams params) {
      short topologyChangeByte = transport.readByte();
      if (topologyChangeByte == 1)
         readNewTopologyAndHash(transport, params);
   }

   protected void readNewTopologyAndHash(Transport transport, HeaderParams params) {
      final Log localLog = getLog();
      int newTopologyId = transport.readVInt();

      SocketAddress[] addresses = readTopology(transport);

      final short hashFunctionVersion;
      final SocketAddress[][] segmentOwners;
      if (params.clientIntel == ClientIntelligence.HASH_DISTRIBUTION_AWARE.getValue()) {
         // Only read the hash if we asked for it
         hashFunctionVersion = transport.readByte();
         int numSegments = transport.readVInt();
         segmentOwners = new SocketAddress[numSegments][];
         if (hashFunctionVersion > 0) {
            for (int i = 0; i < numSegments; i++) {
               short numOwners = transport.readByte();
               segmentOwners[i] = new SocketAddress[numOwners];
               for (int j = 0; j < numOwners; j++) {
                  int memberIndex = transport.readVInt();
                  segmentOwners[i][j] = addresses[memberIndex];
               }
            }
         }
      } else {
         hashFunctionVersion = -1;
         segmentOwners = null;
      }

      TransportFactory transportFactory = transport.getTransportFactory();
      int currentTopology = transportFactory.getTopologyId(params.cacheName);
      int topologyAge = transportFactory.getTopologyAge();
      if (params.topologyAge == topologyAge && currentTopology != newTopologyId) {
         params.topologyId.set(newTopologyId);
         List<SocketAddress> addressList = Arrays.asList(addresses);
         if (localLog.isInfoEnabled()) {
            localLog.newTopology(transport.getRemoteSocketAddress(), newTopologyId, topologyAge,
               addresses.length, new HashSet<>(addressList));
         }
         transportFactory.updateServers(addressList, params.cacheName, false);
         if (hashFunctionVersion >= 0) {
            if (trace) {
               if (hashFunctionVersion == 0)
                  localLog.trace("Not using a consistent hash function (hash function version == 0).");
               else
                  localLog.tracef("Updating client hash function with %s number of segments", segmentOwners.length);
            }
            transportFactory.updateHashFunction(segmentOwners,
                  segmentOwners.length, hashFunctionVersion, params.cacheName, params.topologyId);
         }
      } else {
         if (trace)
            localLog.tracef("Outdated topology received (topology id = %s, topology age = %s), so ignoring it: %s",
               newTopologyId, topologyAge, Arrays.toString(addresses));
      }
   }

   private SocketAddress[] readTopology(Transport transport) {
      int clusterSize = transport.readVInt();
      SocketAddress[] addresses = new SocketAddress[clusterSize];
      for (int i = 0; i < clusterSize; i++) {
         String host = transport.readString();
         int port = transport.readUnsignedShort();
         addresses[i] = InetSocketAddress.createUnresolved(host, port);
      }
      return addresses;
   }

}
