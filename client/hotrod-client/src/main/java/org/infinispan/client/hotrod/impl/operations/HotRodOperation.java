package org.infinispan.client.hotrod.impl.operations;

import net.jcip.annotations.Immutable;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generic Hot Rod operation. It is aware of {@link org.infinispan.client.hotrod.Flag}s and it is targeted against a
 * cache name. This base class encapsulates the knowledge of writing and reading a header, as described in the
 * <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol specification</a>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Immutable
public abstract class HotRodOperation implements HotRodConstants {

   static final AtomicLong MSG_ID = new AtomicLong();

   private static final Log log = LogFactory.getLog(HotRodOperation.class);

   protected final Flag[] flags;

   protected final byte[] cacheName;

   protected final AtomicInteger topologyId;
   
   private static final byte NO_TX = 0;
   private static final byte XA_TX = 1;


   protected HotRodOperation(Flag[] flags, byte[] cacheName, AtomicInteger topologyId) {
      this.flags = flags;
      this.cacheName = cacheName;
      this.topologyId = topologyId;
   }

   public abstract Object execute();

   protected final long writeHeader(Transport transport, short operationCode) {
      transport.writeByte(HotRodConstants.REQUEST_MAGIC);
      long messageId = MSG_ID.incrementAndGet();
      transport.writeVLong(messageId);
      transport.writeByte(HotRodConstants.HOTROD_VERSION);
      transport.writeByte(operationCode);
      transport.writeArray(cacheName);

      int flagInt = 0;
      if (flags != null) {
         for (Flag flag : flags) {
            flagInt = flag.getFlagInt() | flagInt;
         }
      }
      transport.writeVInt(flagInt);
      transport.writeByte(CLIENT_INTELLIGENCE_HASH_DISTRIBUTION_AWARE);
      transport.writeVInt(topologyId.get());
      //todo change once TX support is added
      transport.writeByte(NO_TX);
      if (log.isTraceEnabled()) {
         log.trace("wrote header for message %d. Operation code: %#04x. Flags: %#x", messageId, operationCode, flagInt);
      }
      return messageId;
   }

   /**
    * Magic	| Message Id | Op code | Status | Topology Change Marker
    */
   protected short readHeaderAndValidate(Transport transport, long messageId, short opRespCode) {
      short magic = transport.readByte();
      if (magic != HotRodConstants.RESPONSE_MAGIC) {
         String message = "Invalid magic number. Expected %#x and received %#x";
         log.error(message, HotRodConstants.RESPONSE_MAGIC, magic);
         throw new InvalidResponseException(String.format(message, HotRodConstants.RESPONSE_MAGIC, magic));
      }
      long receivedMessageId = transport.readVLong();
      if (receivedMessageId != messageId) {
         String message = "Invalid message id. Expected %d and received %d";
         log.error(message, messageId, receivedMessageId);
         throw new InvalidResponseException(String.format(message, messageId, receivedMessageId));
      }
      if (log.isTraceEnabled()) {
         log.trace("Received response for message id: %d", receivedMessageId);
      }
      short receivedOpCode = transport.readByte();
      if (receivedOpCode != opRespCode) {
         if (receivedOpCode == HotRodConstants.ERROR_RESPONSE) {
            checkForErrorsInResponseStatus(transport.readByte(), messageId, transport);
         }
         throw new InvalidResponseException(String.format(
               "Invalid response operation. Expected %#x and received %#x",
               opRespCode, receivedOpCode));
      }
      if (log.isTraceEnabled()) {
         log.trace("Received operation code is: %#04x", receivedOpCode);
      }
      short status = transport.readByte();
      // There's not need to check for errors in status here because if there
      // was an error, the server would have replied with error response op code.
      readNewTopologyIfPresent(transport);
      return status;
   }

   protected void checkForErrorsInResponseStatus(short status, long messageId, Transport transport) {
      final boolean isTrace = log.isTraceEnabled();
      if (isTrace) log.trace("Received operation status: %#x", status);

      switch ((int) status) {
         case HotRodConstants.INVALID_MAGIC_OR_MESSAGE_ID_STATUS:
         case HotRodConstants.REQUEST_PARSING_ERROR_STATUS:
         case HotRodConstants.UNKNOWN_COMMAND_STATUS:
         case HotRodConstants.SERVER_ERROR_STATUS:
         case HotRodConstants.COMMAND_TIMEOUT_STATUS:
         case HotRodConstants.UNKNOWN_VERSION_STATUS: {
            readNewTopologyIfPresent(transport);
            String msgFromServer = transport.readString();
            if (status == HotRodConstants.COMMAND_TIMEOUT_STATUS && isTrace) {
               log.trace("Server-side timeout performing operation: %s", msgFromServer);
            } else {
               log.warn("Error received from the server: %s", msgFromServer);
            }
            throw new HotRodClientException(msgFromServer, messageId, status);
         }
         default: {
            throw new IllegalStateException(String.format("Unknown status: %#04x", status));
         }
      }
   }

   private void readNewTopologyIfPresent(Transport transport) {
      short topologyChangeByte = transport.readByte();
      if (topologyChangeByte == 1)
         readNewTopologyAndHash(transport, topologyId);
   }

   private void readNewTopologyAndHash(Transport transport, AtomicInteger topologyId) {
      int newTopologyId = transport.readVInt();
      topologyId.set(newTopologyId);
      int numKeyOwners = transport.readUnsignedShort();
      short hashFunctionVersion = transport.readByte();
      int hashSpace = transport.readVInt();
      int clusterSize = transport.readVInt();

      if (log.isTraceEnabled()) {
         log.trace("Topology change request: newTopologyId=%d, numKeyOwners=%d, " +
                   "hashFunctionVersion=%d, hashSpaceSize=%d, clusterSize=%d",
             newTopologyId, numKeyOwners, hashFunctionVersion, hashSpace, clusterSize);
      }

      LinkedHashMap<InetSocketAddress, Integer> servers2HashCode = new LinkedHashMap<InetSocketAddress, Integer>();

      for (int i = 0; i < clusterSize; i++) {
         String host = transport.readString();
         int port = transport.readUnsignedShort();
         if (log.isTraceEnabled()) {
            log.trace("Server read: %s:%d", host, port);
         }
         int hashCode = transport.read4ByteInt();
         servers2HashCode.put(new InetSocketAddress(host, port), hashCode);
         if (log.isTraceEnabled()) {
            log.trace("Hash code is: %d", hashCode);
         }
      }
      if (log.isInfoEnabled()) {
         log.info("New topology: %s", servers2HashCode);
      }
      transport.getTransportFactory().updateServers(servers2HashCode.keySet());
      if (hashFunctionVersion == 0) {
         if (log.isTraceEnabled())
            log.trace("Not using a consistent hash function (hash function version == 0).");
      } else {
         transport.getTransportFactory().updateHashFunction(servers2HashCode, numKeyOwners, hashFunctionVersion, hashSpace);
      }
   }
}
