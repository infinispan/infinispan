package org.infinispan.client.hotrod.impl;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.exceptions.TimeoutException;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public class HotrodOperationsImpl implements HotrodOperations, HotrodConstants {

   private static Log log = LogFactory.getLog(HotrodOperationsImpl.class);

   private final byte[] cacheNameBytes;
   private static final AtomicLong MSG_ID = new AtomicLong();
   private static final AtomicInteger TOPOLOGY_ID = new AtomicInteger();
   private TransportFactory transportFactory;
   private byte clientIntelligence = CLIENT_INTELLIGENCE_HASH_DISTRIBUTION_AWARE;

   public HotrodOperationsImpl(String cacheName, TransportFactory transportFactory) {
      cacheNameBytes = cacheName.getBytes(); //todo add charset here
      this.transportFactory = transportFactory;
   }

   public byte[] get(byte[] key, Flag[] flags) {
      Transport transport = transportFactory.getTransport(key);
      try {
         short status = sendKeyOperation(key, transport, GET_REQUEST, flags, GET_RESPONSE);
         if (status == KEY_DOES_NOT_EXIST_STATUS) {
            return null;
         }
         if (status == NO_ERROR_STATUS) {
            return transport.readArray();
         }
      } finally {
         releaseTransport(transport);
      }
      throw new IllegalStateException("We should not reach here!");
   }

   public byte[] remove(byte[] key, Flag[] flags) {
      Transport transport = transportFactory.getTransport(key);
      try {
         short status = sendKeyOperation(key, transport, REMOVE_REQUEST, flags, REMOVE_RESPONSE);
         if (status == KEY_DOES_NOT_EXIST_STATUS) {
            return null;
         } else if (status == NO_ERROR_STATUS) {
            return returnPossiblePrevValue(transport, flags);
         }
      } finally {
         releaseTransport(transport);
      }
      throw new IllegalStateException("We should not reach here!");
   }

   public boolean containsKey(byte[] key, Flag... flags) {
      Transport transport = transportFactory.getTransport(key);
      try {
         short status = sendKeyOperation(key, transport, CONTAINS_KEY_REQUEST, flags, CONTAINS_KEY_RESPONSE);
         if (status == KEY_DOES_NOT_EXIST_STATUS) {
            return false;
         } else if (status == NO_ERROR_STATUS) {
            return true;
         }
      } finally {
         releaseTransport(transport);
      }
      throw new IllegalStateException("We should not reach here!");
   }

   public BinaryVersionedValue getWithVersion(byte[] key, Flag... flags) {
      Transport transport = transportFactory.getTransport(key);
      try {
         short status = sendKeyOperation(key, transport, GET_WITH_VERSION, flags, GET_WITH_VERSION_RESPONSE);
         if (status == KEY_DOES_NOT_EXIST_STATUS) {
            return null;
         }
         if (status == NO_ERROR_STATUS) {
            long version = transport.readLong();
            if (log.isTraceEnabled()) {
               log.trace("Received version: " + version);
            }
            byte[] value = transport.readArray();
            return new BinaryVersionedValue(version, value);
         }
      } finally {
         releaseTransport(transport);
      }
      throw new IllegalStateException("We should not reach here!");
   }


   public byte[] put(byte[] key, byte[] value, int lifespan, int maxIdle, Flag... flags) {
      Transport transport = transportFactory.getTransport(key);
      try {
         short status = sendPutOperation(key, value, transport, PUT_REQUEST, PUT_RESPONSE, lifespan, maxIdle, flags);
         if (status != NO_ERROR_STATUS) {
            throw new InvalidResponseException("Unexpected response status: " + Integer.toHexString(status));
         }
         return returnPossiblePrevValue(transport, flags);
      } finally {
         releaseTransport(transport);
      }
   }

   public byte[] putIfAbsent(byte[] key, byte[] value, int lifespan, int maxIdle, Flag... flags) {
      Transport transport = transportFactory.getTransport(key);
      try {
         short status = sendPutOperation(key, value, transport, PUT_IF_ABSENT_REQUEST, PUT_IF_ABSENT_RESPONSE, lifespan, maxIdle, flags);
         if (status == NO_ERROR_STATUS || status == NOT_PUT_REMOVED_REPLACED_STATUS) {
            byte[] bytes = returnPossiblePrevValue(transport, flags);
            if (log.isTraceEnabled()) {
               log.trace("Returning from putIfAbsent: " + Arrays.toString(bytes));
            }
            return bytes;
         }
      } finally {
         releaseTransport(transport);
      }
      throw new IllegalStateException("We should not reach here!");
   }

   public byte[] replace(byte[] key, byte[] value, int lifespan, int maxIdle, Flag... flags) {
      Transport transport = transportFactory.getTransport(key);
      try {
         short status = sendPutOperation(key, value, transport, REPLACE_REQUEST, REPLACE_RESPONSE, lifespan, maxIdle, flags);
         if (status == NO_ERROR_STATUS || status == NOT_PUT_REMOVED_REPLACED_STATUS) {
            return returnPossiblePrevValue(transport, flags);
         }
      } finally {
         releaseTransport(transport);
      }
      throw new IllegalStateException("We should not reach here!");
   }

   /**
    * request : [header][key length][key][lifespan][max idle][entry_version][value length][value] response: If
    * ForceReturnPreviousValue has been passed, this responses will contain previous [value length][value] for that key.
    * If the key does not exist or previous was null, value length would be 0. Otherwise, if no ForceReturnPreviousValue
    * was sent, the response would be empty.
    */
   public VersionedOperationResponse replaceIfUnmodified(byte[] key, byte[] value, int lifespan, int maxIdle, long version, Flag... flags) {
      Transport transport = transportFactory.getTransport(key);
      try {
         // 1) write header
         long messageId = writeHeader(transport, REPLACE_IF_UNMODIFIED_REQUEST, flags);

         //2) write message body
         transport.writeArray(key);
         transport.writeVInt(lifespan);
         transport.writeVInt(maxIdle);
         transport.writeLong(version);
         transport.writeArray(value);
         return returnVersionedOperationResponse(transport, messageId, REPLACE_IF_UNMODIFIED_RESPONSE, flags);
      } finally {
         releaseTransport(transport);
      }
   }

   /**
    * Request: [header][key length][key][entry_version]
    */
   public VersionedOperationResponse removeIfUnmodified(byte[] key, long version, Flag... flags) {
      Transport transport = transportFactory.getTransport(key);
      try {
         // 1) write header
         long messageId = writeHeader(transport, REMOVE_IF_UNMODIFIED_REQUEST, flags);

         //2) write message body
         transport.writeArray(key);
         transport.writeLong(version);

         //process response and return
         return returnVersionedOperationResponse(transport, messageId, REMOVE_IF_UNMODIFIED_RESPONSE, flags);

      } finally {
         releaseTransport(transport);
      }
   }

   public void clear(Flag... flags) {
      Transport transport = transportFactory.getTransport();
      try {
         // 1) write header
         long messageId = writeHeader(transport, CLEAR_REQUEST, flags);
         readHeaderAndValidate(transport, messageId, CLEAR_RESPONSE);
      } finally {
         releaseTransport(transport);
      }
   }

   public Map<String, String> stats() {
      Transport transport = transportFactory.getTransport();
      try {
         // 1) write header
         long messageId = writeHeader(transport, STATS_REQUEST);
         readHeaderAndValidate(transport, messageId, STATS_RESPONSE);
         int nrOfStats = transport.readVInt();

         Map<String, String> result = new HashMap<String, String>();
         for (int i = 0; i < nrOfStats; i++) {
            String statName = transport.readString();
            String statValue = transport.readString();
            result.put(statName, statValue);
         }
         return result;
      } finally {
         releaseTransport(transport);
      }
   }

   @Override
   public boolean ping() {
      Transport transport = null;
      try {
         transport = transportFactory.getTransport();
         // 1) write header
         long messageId = writeHeader(transport, PING_REQUEST);
         short respStatus = readHeaderAndValidate(transport, messageId, HotrodConstants.PING_RESPONSE);
         if (respStatus == NO_ERROR_STATUS) {
            return true;
         }
         throw new IllegalStateException("Unknown response status: " + Integer.toHexString(respStatus));
      } catch (TransportException te) {
         log.trace("Exception while ping", te);
         return false;
      }
      finally {
         releaseTransport(transport);
      }
   }

   //[header][key length][key][lifespan][max idle][value length][value]

   private short sendPutOperation(byte[] key, byte[] value, Transport transport, short opCode, byte opRespCode, int lifespan, int maxIdle, Flag[] flags) {
      // 1) write header
      long messageId = writeHeader(transport, opCode, flags);

      // 2) write key and value
      transport.writeArray(key);
      transport.writeVInt(lifespan);
      transport.writeVInt(maxIdle);
      transport.writeArray(value);
      transport.flush();

      // 3) now read header

      //return status (not error status for sure)
      return readHeaderAndValidate(transport, messageId, opRespCode);
   }

   /*
    * Magic	| MessageId	| Version | Opcode | CacheNameLength | CacheName | Flags | Client Intelligence | Topology Id
    */

   private long writeHeader(Transport transport, short operationCode, Flag... flags) {
      transport.writeByte(REQUEST_MAGIC);
      long messageId = MSG_ID.incrementAndGet();
      transport.writeVLong(messageId);
      transport.writeByte(HOTROD_VERSION);
      transport.writeByte(operationCode);
      transport.writeArray(cacheNameBytes);

      int flagInt = 0;
      if (flags != null) {
         for (Flag flag : flags) {
            flagInt = flag.getFlagInt() | flagInt;
         }
      }
      transport.writeVInt(flagInt);
      transport.writeByte(clientIntelligence);
      transport.writeVInt(TOPOLOGY_ID.get());
      if (log.isTraceEnabled()) {
         log.trace("wrote header for message " + messageId + ". Operation code: " + operationCode + ". Flags: " + Integer.toHexString(flagInt));
      }
      return messageId;
   }

   /**
    * Magic	| Message Id | Op code | Status | Topology Change Marker
    */
   private short readHeaderAndValidate(Transport transport, long messageId, short opRespCode) {
      short magic = transport.readByte();
      if (magic != RESPONSE_MAGIC) {
         String message = "Invalid magic number. Expected " + Integer.toHexString(RESPONSE_MAGIC) + " and received " + Integer.toHexString(magic);
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
         if (receivedOpCode == ERROR_RESPONSE) {
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
         readNewTopologyAndHash(transport);
      }
      return status;
   }

   private void readNewTopologyAndHash(Transport transport) {
      int newTopologyId = transport.readVInt();
      TOPOLOGY_ID.set(newTopologyId);
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
      transportFactory.updateServers(servers2HashCode.keySet());
      if (hashFunctionVersion == 0) {
         if (log.isTraceEnabled())
            log.trace("Not using a consistent hash function (hash function version == 0).");
      } else {
         transportFactory.updateHashFunction(servers2HashCode, numKeyOwners, hashFunctionVersion, hashSpace);
      }
   }

   private void checkForErrorsInResponseStatus(short status, long messageId, Transport transport) {
      if (log.isTraceEnabled()) {
         log.trace("Received operation status: " + status);
      }
      switch ((int) status) {
         case INVALID_MAGIC_OR_MESSAGE_ID_STATUS:
         case REQUEST_PARSING_ERROR_STATUS:
         case UNKNOWN_COMMAND_STATUS:
         case SERVER_ERROR_STATUS:
         case UNKNOWN_VERSION_STATUS: {
            String msgFromServer = transport.readString();
            if (log.isWarnEnabled()) {
               log.warn("Error status received from the server:" + msgFromServer + " for message id " + messageId);
            }
            throw new HotRodClientException(msgFromServer, messageId, status);
         }
         case COMMAND_TIMEOUT_STATUS: {
            if (log.isTraceEnabled()) {
               log.trace("timeout message received from the server");
            }
            throw new TimeoutException();
         }
         case NO_ERROR_STATUS:
         case KEY_DOES_NOT_EXIST_STATUS:
         case NOT_PUT_REMOVED_REPLACED_STATUS: {
            //don't do anything, these are correct responses
            break;
         }
         default: {
            throw new IllegalStateException("Unknown status: " + Integer.toHexString(status));
         }
      }
   }

   private boolean hasForceReturn(Flag[] flags) {
      if (flags == null) return false;
      for (Flag flag : flags) {
         if (flag == Flag.FORCE_RETURN_VALUE) return true;
      }
      return false;
   }

   private short sendKeyOperation(byte[] key, Transport transport, byte opCode, Flag[] flags, byte opRespCode) {
      // 1) write [header][key length][key]
      long messageId = writeHeader(transport, opCode, flags);
      transport.writeArray(key);
      transport.flush();

      // 2) now read the header
      return readHeaderAndValidate(transport, messageId, opRespCode);
   }

   private byte[] returnPossiblePrevValue(Transport transport, Flag[] flags) {
      if (hasForceReturn(flags)) {
         byte[] bytes = transport.readArray();
         if (log.isTraceEnabled()) log.trace("Previous value bytes is: " + Arrays.toString(bytes));
         //0-length response means null
         return bytes.length == 0 ? null : bytes;
      } else {
         return null;
      }
   }

   private void releaseTransport(Transport transport) {
      if (transport != null)
         transportFactory.releaseTransport(transport);
   }

   private VersionedOperationResponse returnVersionedOperationResponse(Transport transport, long messageId, byte response, Flag[] flags) {
      //3) ...
      short respStatus = readHeaderAndValidate(transport, messageId, response);

      //4 ...
      VersionedOperationResponse.RspCode code;
      if (respStatus == NO_ERROR_STATUS) {
         code = VersionedOperationResponse.RspCode.SUCCESS;
      } else if (respStatus == NOT_PUT_REMOVED_REPLACED_STATUS) {
         code = VersionedOperationResponse.RspCode.MODIFIED_KEY;
      } else if (respStatus == KEY_DOES_NOT_EXIST_STATUS) {
         code = VersionedOperationResponse.RspCode.NO_SUCH_KEY;
      } else {
         throw new IllegalStateException("Unknown response status: " + Integer.toHexString(respStatus));
      }
      byte[] prevValue = returnPossiblePrevValue(transport, flags);
      return new VersionedOperationResponse(prevValue, code);
   }
}
