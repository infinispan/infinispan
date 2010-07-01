package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.HotRodTimeoutException;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Helper class for factorizing common parts of read/write operations.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class HotRodOperationsHelper {
   static Log log = LogFactory.getLog(HotRodOperationsHelper.class);
   static final AtomicLong MSG_ID = new AtomicLong();
   final static byte CLIENT_INTELLIGENCE = HotRodConstants.CLIENT_INTELLIGENCE_HASH_DISTRIBUTION_AWARE;

   public static long writeHeader(Transport transport, short operationCode, String cacheName, AtomicInteger topologyId, Flag... flags) {
      transport.writeByte(HotRodConstants.REQUEST_MAGIC);
      long messageId = MSG_ID.incrementAndGet();
      transport.writeVLong(messageId);
      transport.writeByte(HotRodConstants.HOTROD_VERSION);
      transport.writeByte(operationCode);
      transport.writeString(cacheName);

      int flagInt = 0;
      if (flags != null) {
         for (Flag flag : flags) {
            flagInt = flag.getFlagInt() | flagInt;
         }
      }
      transport.writeVInt(flagInt);
      transport.writeByte(CLIENT_INTELLIGENCE);
      transport.writeVInt(topologyId.get());
      if (log.isTraceEnabled()) {
         log.trace("wrote header for message " + messageId + ". Operation code: " + operationCode + ". Flags: " + Integer.toHexString(flagInt));
      }
      return messageId;
   }

   /**
    * Magic	| Message Id | Op code | Status | Topology Change Marker
    */
   public static short readHeaderAndValidate(Transport transport, long messageId, short opRespCode, AtomicInteger topologyId) {
      short magic = transport.readByte();
      if (magic != HotRodConstants.RESPONSE_MAGIC) {
         String message = "Invalid magic number. Expected " + Integer.toHexString(HotRodConstants.RESPONSE_MAGIC) + " and received " + Integer.toHexString(magic);
         log.error(message);
         throw new InvalidResponseException(message);
      }
      long receivedMessageId = transport.readVLong();
      if (receivedMessageId != messageId) {
         String message = "Invalid message id. Expected " + Long.toHexString(messageId) + " and received " + Long.toHexString(receivedMessageId);
         log.error(message);
         throw new InvalidResponseException(message);
      }
      if (log.isTraceEnabled()) {
         log.trace("Received response for message id: " + receivedMessageId);
      }
      short receivedOpCode = transport.readByte();
      if (receivedOpCode != opRespCode) {
         if (receivedOpCode == HotRodConstants.ERROR_RESPONSE) {
            checkForErrorsInResponseStatus(transport.readByte(), messageId, transport);
            throw new IllegalStateException("Error expected! (i.e. exception in the prev statement)");
         }
         throw new InvalidResponseException("Invalid response operation. Expected " + Integer.toHexString(opRespCode) + " and received " + Integer.toHexString(receivedOpCode));
      }
      if (log.isTraceEnabled()) {
         log.trace("Received operation code is: " + receivedOpCode);
      }
      short status = transport.readByte();
      checkForErrorsInResponseStatus(status, messageId, transport);
      short topologyChangeByte = transport.readByte();
      if (topologyChangeByte == 1) {
         readNewTopologyAndHash(transport, topologyId);
      }
      return status;
   }

   static void readNewTopologyAndHash(Transport transport, AtomicInteger topologyId) {
      int newTopologyId = transport.readVInt();
      topologyId.set(newTopologyId);
      int numKeyOwners = transport.readUnsignedShort();
      short hashFunctionVersion = transport.readByte();
      int hashSpace = transport.readVInt();
      int clusterSize = transport.readVInt();

      if (log.isTraceEnabled()) {
         log.trace("Topology change request: newTopologyId=" + newTopologyId + ", numKeyOwners=" + numKeyOwners +
               ", hashFunctionVersion=" + hashFunctionVersion + ", hashSpaceSize=" + hashSpace + ", clusterSize=" + clusterSize);
      }

      LinkedHashMap<InetSocketAddress, Integer> servers2HashCode = new LinkedHashMap<InetSocketAddress, Integer>();

      for (int i = 0; i < clusterSize; i++) {
         String host = transport.readString();
         int port = transport.readUnsignedShort();
         if (log.isTraceEnabled()) {
            log.trace("Server read:" + host + ":" + port);
         }
         int hashCode = transport.read4ByteInt();
         servers2HashCode.put(new InetSocketAddress(host, port), hashCode);
         if (log.isTraceEnabled()) {
            log.trace("Hash code is: " + hashCode);
         }
      }
      if (log.isInfoEnabled()) {
         log.info("New topology: " + servers2HashCode);
      }
      transport.getTransportFactory().updateServers(servers2HashCode.keySet());
      if (hashFunctionVersion == 0) {
         if (log.isTraceEnabled())
            log.trace("Not using a consistent hash function (hash function version == 0).");
      } else {
         transport.getTransportFactory().updateHashFunction(servers2HashCode, numKeyOwners, hashFunctionVersion, hashSpace);
      }
   }

   static void checkForErrorsInResponseStatus(short status, long messageId, Transport transport) {
      if (log.isTraceEnabled()) {
         log.trace("Received operation status: " + status);
      }
      switch ((int) status) {
         case HotRodConstants.INVALID_MAGIC_OR_MESSAGE_ID_STATUS:
         case HotRodConstants.REQUEST_PARSING_ERROR_STATUS:
         case HotRodConstants.UNKNOWN_COMMAND_STATUS:
         case HotRodConstants.SERVER_ERROR_STATUS:
         case HotRodConstants.UNKNOWN_VERSION_STATUS: {
            String msgFromServer = transport.readString();
            if (log.isWarnEnabled()) {
               log.warn("Error status received from the server:" + msgFromServer + " for message id " + messageId);
            }
            throw new HotRodClientException(msgFromServer, messageId, status);
         }
         case HotRodConstants.COMMAND_TIMEOUT_STATUS: {
            if (log.isTraceEnabled()) {
               log.trace("timeout message received from the server");
            }
            throw new HotRodTimeoutException();
         }
         case HotRodConstants.NO_ERROR_STATUS:
         case HotRodConstants.KEY_DOES_NOT_EXIST_STATUS:
         case HotRodConstants.NOT_PUT_REMOVED_REPLACED_STATUS: {
            //don't do anything, these are correct responses
            break;
         }
         default: {
            throw new IllegalStateException("Unknown status: " + Integer.toHexString(status));
         }
      }
   }
}
