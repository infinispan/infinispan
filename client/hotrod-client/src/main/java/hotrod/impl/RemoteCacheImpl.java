package hotrod.impl;

import hotrod.ClusterTopologyListener;
import hotrod.Flag;
import hotrod.HotRodClientException;
import hotrod.TimeoutException;
import hotrod.impl.VersionedEntry;
import hotrod.impl.RemoteCacheSpi;
import hotrod.RemoteCacheManager;
import hotrod.impl.transport.TransportFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public class RemoteCacheImpl implements RemoteCacheSpi, HotrodConstants {

   private final String cacheName;
   private final byte[] cacheNameBytes;
   private static final AtomicLong MSG_ID = new AtomicLong();
   private TransportFactory transportFactory;

   public RemoteCacheImpl(String cacheName, TransportFactory transportFactory) {
      this.cacheName = cacheName;
      cacheNameBytes = cacheName.getBytes(); //todo add charset here
      this.transportFactory = transportFactory;
   }

   public byte[] get(byte[] key) {
      Transport transport = getTransport();
      try {
         byte status = sendKeyOperation(key, transport, GET_REQUEST);
         if (status == KEY_DOES_NOT_EXIST_STATUS) {
            return null;
         }
         if (status == NO_ERROR_STATUS) {
            int responseLength = transport.readVInt();
            byte[] result = new byte[responseLength];
            return transport.readByteArray(result);
         } 
      } finally {
         transport.release();
      }
      throw new IllegalStateException("We should not reach here!");
   }

   public boolean remove(byte[] key) {
      Transport transport = getTransport();
      try {
         byte status = sendKeyOperation(key, transport, REMOVE_REQUEST);
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

   public boolean contains(byte[] key) {
      Transport transport = getTransport();
      try {
         byte status = sendKeyOperation(key, transport, CONTAINS_KEY_REQUEST);
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

   public VersionedEntry getVersionedCacheEntry(byte[] key) {
      Transport transport = getTransport();
      try {
         byte status = sendKeyOperation(key, transport, GET_WITH_CAS_REQUEST);
         if (status == KEY_DOES_NOT_EXIST_STATUS) {
            return null;
         }
         if (status == NO_ERROR_STATUS) {
            long cas = transport.readVLong();
            int responseLength = transport.readVInt();
            byte[] value = new byte[responseLength];
            return new VersionedEntry(cas, key, value);
         }
      } finally {
         transport.release();
      }
      throw new IllegalStateException("We should not reach here!");
   }

   //[header][key length][key][lifespan][max idle][value length][value]
   public void put(byte[] key, byte[] value) {
      Transport transport = getTransport();
      try {
         byte status = sendPutOperation(key, value, transport, PUT_REQUEST);
         if (status != NO_ERROR_STATUS) {
            throw new InvalidResponseException("Unexpected response status: " + Integer.toHexString(status));
         }
      } finally {
         transport.release();
      }
   }

   public boolean putIfAbsent(byte[] key, byte[] value) {
      Transport transport = getTransport();
      try {
         byte status = sendPutOperation(key, value, transport, PUT_IF_ABSENT_REQUEST);
         if (status == NO_ERROR_STATUS) {
            return true;
         } else if (status == NOT_PUT_REMOVED_REPLACED_STATUS) {
            return false;
         }
      } finally {
         transport.release();
      }
      throw new IllegalStateException("We should not reach here!");
   }

   public boolean replace(byte[] key, byte[] value) {
      return false;
   }

   public VersionedOperationResponse replaceIfUnmodified(byte[] key, byte[] value, long version) {
      return null;  // TODO: Customise this generated block
   }

   public VersionedOperationResponse removeIfUnmodified(byte[] key, long version) {
      return null;  // TODO: Customise this generated block
   }

   public void putForExternalRead(byte[] key, byte[] value) {
      // TODO: Customise this generated block
   }

   public boolean evict(byte[] key) {
      return false;  // TODO: Customise this generated block
   }

   public void clear() {
      // TODO: Customise this generated block
   }

   public String stats() {
      return null;  // TODO: Customise this generated block
   }

   public String stats(String paramName) {
      return null;  // TODO: Customise this generated block
   }

   public void addClusterTopologyListener(ClusterTopologyListener listener) {
      // TODO: Customise this generated block
   }

   public boolean removeClusterTopologyListener(ClusterTopologyListener listener) {
      return false;  // TODO: Customise this generated block
   }

   public RemoteCacheManager getRemoteCacheFactory() {
      return null;  // TODO: Customise this generated block
   }

   public Transport getTransport() {
      return transportFactory.getTransport();
   }


   private byte sendKeyOperation(byte[] key, Transport transport, byte code) {
      long messageId = writeHeader(transport, code);
      transport.writeByteArray(key);
      transport.flush();
      byte status = readHeaderAndValidate(transport, messageId, code);

      //this will make sure that we don't have an error
      processResponseStatus(status, messageId);
      return status;
   }

   private byte sendPutOperation(byte[] key, byte[] value, Transport transport, byte code) {
      long messageId = writeHeader(transport, code);
      transport.writeByteArray(key);
      //todo - lifespan and max_idle
      transport.writeVInt(0); //lifespan
      transport.writeVInt(0); //max_idle
      transport.writeByteArray(value);
      transport.flush();
      byte status = readHeaderAndValidate(transport, messageId, code);
      processResponseStatus(status, messageId);
      return status;
   }

   /*
    * Magic	| MessageId	| Version | Opcode | CacheNameLength | CacheName | Flags
    */
   private long writeHeader(Transport transport, byte operationCode, Flag... flags) {
      transport.appendUnsignedByte(REQUEST_MAGIC);
      long messageId = MSG_ID.incrementAndGet();
      transport.writeVLong(messageId);
      transport.writeByteArray(HOTROD_VERSION, operationCode);
      transport.writeVInt(cacheNameBytes.length);
      transport.writeByteArray(cacheNameBytes);
      int flagInt = 0;
      for (Flag flag: flags) {
         flagInt = flag.getFlagInt() | flagInt;
      }
      transport.writeVInt(flagInt);
      return messageId;
   }

   private byte readHeaderAndValidate(Transport transport, long messageId, byte opCode) {
      byte magic = transport.readByte();
      if (magic != RESPONSE_MAGIC) {
         throw new InvalidResponseException("Invalid magic number. Expected " + RESPONSE_MAGIC + " and received " + Integer.toHexString(magic));
      }
      long receivedMessageId = transport.readVLong();
      if (receivedMessageId != messageId) {
         throw new InvalidResponseException("Invalid message id. Expected " + messageId + " and received " + Long.toHexString(receivedMessageId));
      }
      byte receivedOpCode = transport.readByte();
      if (receivedOpCode != opCode) {
         throw new InvalidResponseException("Invalid response operation. Expected " + opCode + " and received " + Integer.toHexString(receivedOpCode));
      }
      return transport.readByte();
   }

   private void processResponseStatus(byte status, long messageId) {
      switch ((int)status) {
         case INVALID_MAGIC_OR_MESSAGE_ID_STATUS:
         case REQUEST_PARSING_ERROR_STATUS:
         case UNKNOWN_COMMAND_STATUS:
         case SERVER_ERROR_STATUS:
         case UNKNOWN_VERSION_STATUS: {
            throw new HotRodClientException(messageId, status);
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
}
