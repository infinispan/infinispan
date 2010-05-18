package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.BinaryVersionedValue;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class HotRodOperationsImpl implements HotRodOperations, HotRodConstants {

   private static Log log = LogFactory.getLog(HotRodOperationsImpl.class);

   private final String cacheName;
   private TransportFactory transportFactory;
   private final AtomicInteger topologyId;

   public HotRodOperationsImpl(String cacheName, TransportFactory transportFactory, AtomicInteger topologyId) {
      this.cacheName = cacheName;
      this.transportFactory = transportFactory;
      this.topologyId = topologyId;
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
         long messageId = HotRodOperationsHelper.writeHeader(transport, REPLACE_IF_UNMODIFIED_REQUEST, cacheName, topologyId, flags);

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
         long messageId = HotRodOperationsHelper.writeHeader(transport, REMOVE_IF_UNMODIFIED_REQUEST, cacheName, topologyId, flags);

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
         long messageId = HotRodOperationsHelper.writeHeader(transport, CLEAR_REQUEST, cacheName, topologyId, flags);
         HotRodOperationsHelper.readHeaderAndValidate(transport, messageId, CLEAR_RESPONSE, topologyId);
      } finally {
         releaseTransport(transport);
      }
   }

   public Map<String, String> stats() {
      Transport transport = transportFactory.getTransport();
      try {
         // 1) write header
         long messageId = HotRodOperationsHelper.writeHeader(transport, STATS_REQUEST, cacheName, topologyId);
         HotRodOperationsHelper.readHeaderAndValidate(transport, messageId, STATS_RESPONSE, topologyId);
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

   //[header][key length][key][lifespan][max idle][value length][value]

   private short sendPutOperation(byte[] key, byte[] value, Transport transport, short opCode, byte opRespCode, int lifespan, int maxIdle, Flag[] flags) {
      // 1) write header
      long messageId = HotRodOperationsHelper.writeHeader(transport, opCode, cacheName, topologyId, flags);

      // 2) write key and value
      transport.writeArray(key);
      transport.writeVInt(lifespan);
      transport.writeVInt(maxIdle);
      transport.writeArray(value);
      transport.flush();

      // 3) now read header

      //return status (not error status for sure)
      return HotRodOperationsHelper.readHeaderAndValidate(transport, messageId, opRespCode, topologyId);
   }

   /*
    * Magic	| MessageId	| Version | Opcode | CacheNameLength | CacheName | Flags | Client Intelligence | Topology Id
    */

   private boolean hasForceReturn(Flag[] flags) {
      if (flags == null) return false;
      for (Flag flag : flags) {
         if (flag == Flag.FORCE_RETURN_VALUE) return true;
      }
      return false;
   }

   private short sendKeyOperation(byte[] key, Transport transport, byte opCode, Flag[] flags, byte opRespCode) {
      // 1) write [header][key length][key]
      long messageId = HotRodOperationsHelper.writeHeader(transport, opCode, cacheName, topologyId, flags);
      transport.writeArray(key);
      transport.flush();

      // 2) now read the header
      return HotRodOperationsHelper.readHeaderAndValidate(transport, messageId, opRespCode, topologyId);
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
      short respStatus = HotRodOperationsHelper.readHeaderAndValidate(transport, messageId, response, topologyId);

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
