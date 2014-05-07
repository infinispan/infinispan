package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryModifiedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.exceptions.RemoteNodeSuspectException;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Either;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.infinispan.commons.util.Util.*;

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
   public HeaderParams writeHeader(Transport transport, HeaderParams params) {
      return writeHeader(transport, params, HotRodConstants.VERSION_20);
   }

   protected HeaderParams writeHeader(
         Transport transport, HeaderParams params, byte version) {
      transport.writeByte(HotRodConstants.REQUEST_MAGIC);
      transport.writeVLong(params.messageId(MSG_ID.incrementAndGet()).messageId);
      transport.writeByte(version);
      transport.writeByte(params.opCode);
      transport.writeArray(params.cacheName);
      int joinedFlags = HeaderParams.joinFlags(params.flags);
      transport.writeVInt(joinedFlags);
      transport.writeByte(params.clientIntel);
      transport.writeVInt(params.topologyId.get());

      if (trace)
         getLog().tracef("Wrote header for message %d. Operation code: %#04x. Flags: %#x",
            params.messageId, params.opCode, joinedFlags);

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
   public ClientEvent readEvent(Transport transport, byte[] expectedListenerId, Marshaller marshaller) {
      readMagic(transport);
      readMessageId(transport, null);
      short eventTypeId = transport.readByte();
      return readPartialEvent(transport, expectedListenerId, marshaller, eventTypeId);
   }

   private ClientEvent readPartialEvent(Transport transport, byte[] expectedListenerId, Marshaller marshaller, short eventTypeId) {
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
         default:
            throw log.unknownEvent(eventTypeId);
      }

      byte[] listenerId = transport.readArray();
      if (!Arrays.equals(listenerId, expectedListenerId))
         throw log.unexpectedListenerId(printArray(listenerId), printArray(expectedListenerId));

      short isCustom = transport.readByte();

      if (isCustom == 1) {
         final Object eventData = MarshallerUtil.bytes2obj(marshaller, transport.readArray());
         return createCustomEvent(eventData, eventType);
      } else {
         switch (eventType) {
            case CLIENT_CACHE_ENTRY_CREATED:
               Object createdKey = MarshallerUtil.bytes2obj(marshaller, transport.readArray());
               long createdDataVersion = transport.readLong();
               return createCreatedEvent(createdKey, createdDataVersion);
            case CLIENT_CACHE_ENTRY_MODIFIED:
               Object modifiedKey = MarshallerUtil.bytes2obj(marshaller, transport.readArray());
               long modifiedDataVersion = transport.readLong();
               return createModifiedEvent(modifiedKey, modifiedDataVersion);
            case CLIENT_CACHE_ENTRY_REMOVED:
               Object removedKey = MarshallerUtil.bytes2obj(marshaller, transport.readArray());
               return createRemovedEvent(removedKey);
            default:
               throw log.unknownEvent(eventTypeId);
         }
      }
   }

   @Override
   public Either<Short, ClientEvent> readHeaderOrEvent(Transport transport, HeaderParams params, byte[] expectedListenerId, Marshaller marshaller) {
      readMagic(transport);
      readMessageId(transport, null);
      short opCode = transport.readByte();
      switch (opCode) {
         case CACHE_ENTRY_CREATED_EVENT_RESPONSE:
         case CACHE_ENTRY_MODIFIED_EVENT_RESPONSE:
         case CACHE_ENTRY_REMOVED_EVENT_RESPONSE:
            ClientEvent clientEvent = readPartialEvent(transport, expectedListenerId, marshaller, opCode);
            return Either.newRight(clientEvent);
         default:
            return Either.newLeft(readPartialHeader(transport, params, opCode));
      }
   }

   private ClientEvent createRemovedEvent(final Object key) {
      return new ClientCacheEntryRemovedEvent() {
         @Override public Object getKey() { return key; }
         @Override public Type getType() { return Type.CLIENT_CACHE_ENTRY_REMOVED; }
         @Override
         public String toString() {
            return "ClientCacheEntryRemovedEvent(" + "key=" + key + ")";
         }
      };
   }

   private ClientCacheEntryModifiedEvent createModifiedEvent(final Object key, final long dataVersion) {
      return new ClientCacheEntryModifiedEvent() {
         @Override public Object getKey() { return key; }
         @Override public long getVersion() { return dataVersion; }
         @Override public Type getType() { return Type.CLIENT_CACHE_ENTRY_MODIFIED; }
         @Override
         public String toString() {
            return "ClientCacheEntryModifiedEvent(" + "key=" + key
                  + ",dataVersion=" + dataVersion + ")";
         }
      };
   }

   private ClientCacheEntryCreatedEvent<Object> createCreatedEvent(final Object key, final long dataVersion) {
      return new ClientCacheEntryCreatedEvent<Object>() {
         @Override public Object getKey() { return key; }
         @Override public long getVersion() { return dataVersion; }
         @Override public Type getType() { return Type.CLIENT_CACHE_ENTRY_CREATED; }
         @Override
         public String toString() {
            return "ClientCacheEntryCreatedEvent(" + "key=" + key
                  + ",dataVersion=" + dataVersion + ")";
         }
      };
   }

   private ClientCacheEntryCustomEvent<Object> createCustomEvent(final Object eventData, ClientEvent.Type evenType) {
      return new ClientCacheEntryCustomEvent<Object>() {
         @Override public Object getEventData() { return eventData; }
         @Override public Type getType() { return Type.CLIENT_CACHE_ENTRY_CREATED; }
         @Override
         public String toString() {
            return "ClientCacheEntryCustomEvent(" + "eventData=" + eventData + ")";
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
         localLog.tracef("Received response for message id: %d", receivedMessageId);

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
      boolean isTrace = localLog.isTraceEnabled();
      if (isTrace) localLog.tracef("Received operation status: %#x", status);

      try {
         switch (status) {
            case HotRodConstants.INVALID_MAGIC_OR_MESSAGE_ID_STATUS:
            case HotRodConstants.REQUEST_PARSING_ERROR_STATUS:
            case HotRodConstants.UNKNOWN_COMMAND_STATUS:
            case HotRodConstants.SERVER_ERROR_STATUS:
            case HotRodConstants.COMMAND_TIMEOUT_STATUS:
            case HotRodConstants.UNKNOWN_VERSION_STATUS: {
               // If error, the body of the message just contains a message
               String msgFromServer = transport.readString();
               if (status == HotRodConstants.COMMAND_TIMEOUT_STATUS && isTrace) {
                  localLog.tracef("Server-side timeout performing operation: %s", msgFromServer);
               } if (msgFromServer.contains("SuspectException")
                     || msgFromServer.contains("SuspectedException")) {
                  // Handle both Infinispan's and JGroups' suspicions
                  if (isTrace)
                     localLog.tracef("A remote node was suspected while executing messageId=%d. " +
                           "Check if retry possible. Message from server: %s", params.messageId, msgFromServer);
                  // TODO: This will be better handled with its own status id in version 2 of protocol
                  throw new RemoteNodeSuspectException(msgFromServer, params.messageId, status);
               } else {
                  localLog.errorFromServer(msgFromServer);
               }
               throw new HotRodClientException(msgFromServer, params.messageId, status);
            }
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
         readNewTopologyAndHash(transport, params.topologyId);
   }

   protected void readNewTopologyAndHash(Transport transport, AtomicInteger topologyId) {
      final Log localLog = getLog();
      int newTopologyId = transport.readVInt();
      topologyId.set(newTopologyId);

      int clusterSize = transport.readVInt();
      SocketAddress[] addresses = new SocketAddress[clusterSize];
      for (int i = 0; i < clusterSize; i++) {
         String host = transport.readString();
         int port = transport.readUnsignedShort();
         addresses[i] = new InetSocketAddress(host, port);
      }

      short hashFunctionVersion = transport.readByte();
      int numSegments = transport.readVInt();
      SocketAddress[][] segmentOwners = new SocketAddress[numSegments][];
      for (int i = 0; i < numSegments; i++) {
         short numOwners = transport.readByte();
         segmentOwners[i] = new SocketAddress[numOwners];
         for (int j = 0; j < numOwners; j++) {
            int memberIndex = transport.readVInt();
            segmentOwners[i][j] = addresses[memberIndex];
         }
      }

      List<SocketAddress> addressList = Arrays.asList(addresses);
      if (localLog.isInfoEnabled()) {
         localLog.newTopology(transport.getRemoteSocketAddress(), newTopologyId,
               addresses.length, new HashSet<SocketAddress>(addressList));
      }
      transport.getTransportFactory().updateServers(addressList);
      if (hashFunctionVersion == 0) {
         if (trace)
            localLog.trace("Not using a consistent hash function (hash function version == 0).");
      } else {
         transport.getTransportFactory().updateHashFunction(segmentOwners, numSegments, hashFunctionVersion);
      }
   }

}
