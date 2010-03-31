package org.infinispan.client.hotrod.impl;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.exceptions.TimeoutException;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public class HotrodOperationsImpl implements HotrodOperations, HotrodConstants {

   private final byte[] cacheNameBytes;
   private static final AtomicLong MSG_ID = new AtomicLong();
   private TransportFactory transportFactory;
   private byte clientIntelligence;

   public HotrodOperationsImpl(String cacheName, TransportFactory transportFactory) {
      cacheNameBytes = cacheName.getBytes(); //todo add charset here
      this.transportFactory = transportFactory;
   }

   public byte[] get(byte[] key, Flag[] flags) {
      Transport transport = getTransport();
      try {
         short status = sendKeyOperation(key, transport, GET_REQUEST, flags, GET_RESPONSE);
         if (status == KEY_DOES_NOT_EXIST_STATUS) {
            return null;
         }
         if (status == NO_ERROR_STATUS) {
            return transport.readByteArray();
         }
      } finally {
         transport.release();
      }
      throw new IllegalStateException("We should not reach here!");
   }

   public byte[] remove(byte[] key, Flag[] flags) {
      Transport transport = getTransport();
      try {
         short status = sendKeyOperation(key, transport, REMOVE_REQUEST, flags, REMOVE_RESPONSE);
         if (status == KEY_DOES_NOT_EXIST_STATUS) {
            return null;
         } else if (status == NO_ERROR_STATUS) {
            return returnPossiblePrevValue(transport, flags);
         }
      } finally {
         transport.release();
      }
      throw new IllegalStateException("We should not reach here!");
   }

   public boolean containsKey(byte[] key, Flag... flags) {
      Transport transport = getTransport();
      try {
         short status = sendKeyOperation(key, transport, CONTAINS_KEY_REQUEST, flags, CONTAINS_KEY_RESPONSE);
         if (status == KEY_DOES_NOT_EXIST_STATUS) {
            return false;
         } else if (status == NO_ERROR_STATUS) {
            return true;
         }
      } finally {
         transport.release();
      }
      throw new IllegalStateException("We should not reach here!");
   }

   public BinaryVersionedValue getWithVersion(byte[] key, Flag... flags) {
      Transport transport = getTransport();
      try {
         short status = sendKeyOperation(key, transport, GET_WITH_CAS_REQUEST, flags, GET_WITH_CAS_RESPONSE);
         if (status == KEY_DOES_NOT_EXIST_STATUS) {
            return null;
         }
         if (status == NO_ERROR_STATUS) {
            long version = transport.readVLong();
            byte[] value = transport.readByteArray();
            return new BinaryVersionedValue(version, value);
         }
      } finally {
         transport.release();
      }
      throw new IllegalStateException("We should not reach here!");
   }


   public byte[] put(byte[] key, byte[] value, int lifespan, int maxIdle, Flag... flags) {
      Transport transport = getTransport();
      try {
         short status = sendPutOperation(key, value, transport, PUT_REQUEST, PUT_RESPONSE, lifespan, maxIdle, flags);
         if (status != NO_ERROR_STATUS) {
            throw new InvalidResponseException("Unexpected response status: " + Integer.toHexString(status));
         }
         return returnPossiblePrevValue(transport, flags);
      } finally {
         transport.release();
      }
   }

   public byte[] putIfAbsent(byte[] key, byte[] value, int lifespan, int maxIdle, Flag... flags) {
      Transport transport = getTransport();
      try {
         short status = sendPutOperation(key, value, transport, PUT_IF_ABSENT_REQUEST, PUT_IF_ABSENT_RESPONSE, lifespan, maxIdle, flags);
         if (status == NO_ERROR_STATUS) {
            return returnPossiblePrevValue(transport, flags);
         } else if (status == NOT_PUT_REMOVED_REPLACED_STATUS) {
            return null;
         }
      } finally {
         transport.release();
      }
      throw new IllegalStateException("We should not reach here!");
   }

   public byte[] replace(byte[] key, byte[] value, int lifespan, int maxIdle, Flag... flags) {
      return null;
   }

   public VersionedOperationResponse replaceIfUnmodified(byte[] key, byte[] value, int lifespan, int maxIdle, long version, Flag... flags) {
      return null;  // TODO: Customise this generated block
   }

   public VersionedOperationResponse removeIfUnmodified(byte[] key, long version, Flag... flags) {
      return null;  // TODO: Customise this generated block
   }

   public void clear(Flag... flags) {
      // TODO: Customise this generated block
   }

   public Map<String, String> stats() {
      return null;  // TODO: Customise this generated block
   }

   public String stats(String paramName) {
      return null;  // TODO: Customise this generated block
   }

   public Transport getTransport() {
      return transportFactory.getTransport();
   }

   private short sendKeyOperation(byte[] key, Transport transport, byte opCode, Flag[] flags, byte opRespCode) {
      // 1) write [header][key length][key]
      long messageId = writeHeader(transport, opCode, flags);
      transport.writeByteArray(key);
      transport.flush();

      // 2) now read the header
      short status = readHeaderAndValidate(transport, messageId, opCode, opRespCode);

      // 3) process possible error messages
      checkForErrorsInResponseStatus(status, messageId, transport);

      return status;
   }

   @Override
   public boolean ping() {
      return false;  // TODO: Customise this generated block
   }

   //[header][key length][key][lifespan][max idle][value length][value]

   private short sendPutOperation(byte[] key, byte[] value, Transport transport, short opCode, byte opRespCode, int lifespan, int maxIdle, Flag[] flags) {
      // 1) write header
      long messageId = writeHeader(transport, opCode, flags);

      // 2) write key and value
      transport.writeByteArray(key);
      transport.writeVInt(lifespan);
      transport.writeVInt(maxIdle);
      transport.writeByteArray(value);
      transport.flush();

      // 3) now read header
      short status = readHeaderAndValidate(transport, messageId, opCode, opRespCode);
      checkForErrorsInResponseStatus(status, messageId, transport);

      //return status (not error status for sure)
      return status;
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
      transport.writeByteArray(cacheNameBytes);
      int flagInt = 0;
      if (flags != null) {
         for (Flag flag : flags) {
            flagInt = flag.getFlagInt() | flagInt;
         }
      }
      transport.writeVInt(flagInt);
      transport.writeByte(clientIntelligence);
      transport.writeVInt(0);//this will be changed once smarter clients are supported
      return messageId;
   }

   /**
    * Magic	| Message Id | Op code | Status | Topology Change Marker
    */
   private short readHeaderAndValidate(Transport transport, long messageId, short opCode, short opRespCode) {
      short magic = transport.readByte();
      if (magic != RESPONSE_MAGIC) {
         throw new InvalidResponseException("Invalid magic number. Expected " + Integer.toHexString(RESPONSE_MAGIC) + " and received " + Integer.toHexString(magic));
      }
      long receivedMessageId = transport.readVLong();
      if (receivedMessageId != messageId) {
         throw new InvalidResponseException("Invalid message id. Expected " + Long.toHexString(messageId) + " and received " + Long.toHexString(receivedMessageId));
      }
      short receivedOpCode = transport.readByte();
      if (receivedOpCode != opRespCode) {
         if (receivedOpCode == ERROR_RESPONSE) {
            checkForErrorsInResponseStatus(transport.readByte(), messageId, transport);
            throw new IllegalStateException("Error expected! (i.e. exception in the prev statement)");
         }
         throw new InvalidResponseException("Invalid response operation. Expected " + Integer.toHexString(opCode) + " and received " + Integer.toHexString(receivedOpCode));
      }
      short status = transport.readByte();
      transport.readByte(); //todo - this will be changed once we support smarter than basic clients
      return status; 
   }

   private void checkForErrorsInResponseStatus(short status, long messageId, Transport transport) {
      switch ((int) status) {
         case INVALID_MAGIC_OR_MESSAGE_ID_STATUS:
         case REQUEST_PARSING_ERROR_STATUS:
         case UNKNOWN_COMMAND_STATUS:
         case SERVER_ERROR_STATUS:
         case UNKNOWN_VERSION_STATUS: {
            throw new HotRodClientException(transport.readString(), messageId, status);
         }
         case COMMAND_TIMEOUT_STATUS: {
            throw new TimeoutException();
         }
         case NO_ERROR_STATUS:
         case KEY_DOES_NOT_EXIST_STATUS: {
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

   private byte[] returnPossiblePrevValue(Transport transport, Flag[] flags) {
      return hasForceReturn(flags) ? transport.readByteArray() : null;
   }
}
