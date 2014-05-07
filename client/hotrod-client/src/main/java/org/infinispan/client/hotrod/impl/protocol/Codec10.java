package org.infinispan.client.hotrod.impl.protocol;

import static org.infinispan.commons.util.Util.hexDump;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.exceptions.RemoteNodeSuspectException;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Either;

/**
 * A Hot Rod encoder/decoder for version 1.0 of the protocol.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class Codec10 implements Codec {

   private static final Log log = LogFactory.getLog(Codec10.class, Log.class);

   static final AtomicLong MSG_ID = new AtomicLong();

   @Override
   public HeaderParams writeHeader(Transport transport, HeaderParams params) {
      return writeHeader(transport, params, HotRodConstants.VERSION_10);
   }

   protected HeaderParams writeHeader(
            Transport transport, HeaderParams params, byte version) {
      transport.writeByte(HotRodConstants.REQUEST_MAGIC);
      transport.writeVLong(params.messageId(MSG_ID.incrementAndGet()).messageId);
      transport.writeByte(version);
      transport.writeByte(params.opCode);
      transport.writeArray(params.cacheName);

      int flagInt = 0;
      if (params.flags != null) {
         for (Flag flag : params.flags) {
            if (flag.equals(Flag.FORCE_RETURN_VALUE)) // 1.0 / 1.1 servers only understand this flag
               flagInt = flag.getFlagInt();
         }
      }
      transport.writeVInt(flagInt);
      transport.writeByte(params.clientIntel);
      transport.writeVInt(params.topologyId.get());
      //todo change once TX support is added
      transport.writeByte(params.txMarker);
      getLog().tracef("Wrote header for message %d. Operation code: %#04x. Flags: %#x",
                 params.messageId, params.opCode, flagInt);
      return params;
   }

   @Override
   public short readHeader(Transport transport, HeaderParams params) {
      short magic = transport.readByte();
      final Log localLog = getLog();
      boolean isTrace = localLog.isTraceEnabled();
      if (magic != HotRodConstants.RESPONSE_MAGIC) {
         String message = "Invalid magic number. Expected %#x and received %#x";
         localLog.invalidMagicNumber(HotRodConstants.RESPONSE_MAGIC, magic);
         if (isTrace)
            localLog.tracef("Socket dump: %s", hexDump(transport.dumpStream()));
         throw new InvalidResponseException(String.format(message, HotRodConstants.RESPONSE_MAGIC, magic));
      }
      long receivedMessageId = transport.readVLong();
      // If received id is 0, it could be that a failure was noted before the
      // message id was detected, so don't consider it to a message id error
      if (receivedMessageId != params.messageId && receivedMessageId != 0) {
         String message = "Invalid message id. Expected %d and received %d";
         localLog.invalidMessageId(params.messageId, receivedMessageId);
         if (isTrace)
            localLog.tracef("Socket dump: %s", hexDump(transport.dumpStream()));
         throw new InvalidResponseException(String.format(message, params.messageId, receivedMessageId));
      }
      localLog.tracef("Received response for message id: %d", receivedMessageId);

      short receivedOpCode = transport.readByte();
      // Read both the status and new topology (if present),
      // before deciding how to react to error situations.
      short status = transport.readByte();
      readNewTopologyIfPresent(transport, params);

      // Now that all headers values have been read, check the error responses.
      // This avoids situatiations where an exceptional return ends up with
      // the socket containing data from previous request responses.
      if (receivedOpCode != params.opRespCode) {
         if (receivedOpCode == HotRodConstants.ERROR_RESPONSE) {
            checkForErrorsInResponseStatus(transport, params, status);
         }
         throw new InvalidResponseException(String.format(
               "Invalid response operation. Expected %#x and received %#x",
               params.opRespCode, receivedOpCode));
      }
      localLog.tracef("Received operation code is: %#04x", receivedOpCode);

      return status;
   }

   @Override
   public ClientEvent readEvent(Transport transport, byte[] expectedListenerId, Marshaller marshaller) {
      return null;  // No events sent in Hot Rod 1.x protocol
   }

   @Override
   public Either<Short, ClientEvent> readHeaderOrEvent(Transport transport, HeaderParams params, byte[] expectedListenerId, Marshaller marshaller) {
      return null;  // No events sent in Hot Rod 1.x protocol
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
      int numKeyOwners = transport.readUnsignedShort();
      short hashFunctionVersion = transport.readByte();
      int hashSpace = transport.readVInt();
      int clusterSize = transport.readVInt();

      Map<SocketAddress, Set<Integer>> servers2Hash = computeNewHashes(
            transport, localLog, newTopologyId, numKeyOwners,
            hashFunctionVersion, hashSpace, clusterSize);

      Set<SocketAddress> socketAddresses = servers2Hash.keySet();
      if (localLog.isInfoEnabled()) {
         localLog.newTopology(transport.getRemoteSocketAddress(), newTopologyId,
               socketAddresses.size(), socketAddresses);
      }
      transport.getTransportFactory().updateServers(socketAddresses);
      if (hashFunctionVersion == 0) {
         localLog.trace("Not using a consistent hash function (hash function version == 0).");
      } else {
         transport.getTransportFactory().updateHashFunction(
               servers2Hash, numKeyOwners, hashFunctionVersion, hashSpace);
      }
   }

   protected Map<SocketAddress, Set<Integer>> computeNewHashes(Transport transport,
         Log localLog, int newTopologyId, int numKeyOwners,
         short hashFunctionVersion, int hashSpace, int clusterSize) {
      if (localLog.isTraceEnabled()) {
         localLog.tracef("Topology change request: newTopologyId=%d, numKeyOwners=%d, " +
                       "hashFunctionVersion=%d, hashSpaceSize=%d, clusterSize=%d",
                 newTopologyId, numKeyOwners, hashFunctionVersion, hashSpace, clusterSize);
      }

      Map<SocketAddress, Set<Integer>> servers2Hash = new LinkedHashMap<SocketAddress, Set<Integer>>();

      for (int i = 0; i < clusterSize; i++) {
         String host = transport.readString();
         int port = transport.readUnsignedShort();
         int hashCode = transport.read4ByteInt();
         localLog.tracef("Server read: %s:%d - hash code is %d", host, port, hashCode);
         SocketAddress address = new InetSocketAddress(host, port);
         Set<Integer> hashes = servers2Hash.get(address);
         if (hashes == null) {
            hashes = new HashSet<Integer>();
            servers2Hash.put(address, hashes);
         }
         hashes.add(hashCode);
         localLog.tracef("Hash code is: %d", hashCode);
      }
      return servers2Hash;
   }

}
