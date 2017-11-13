package org.infinispan.client.hotrod.impl.protocol;

import static org.infinispan.commons.util.Util.printArray;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.List;

import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryExpiredEvent;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.commons.marshall.Marshaller;

import io.netty.buffer.ByteBuf;

public class Codec21 extends Codec20 {

   private static final Log log = LogFactory.getLog(Codec21.class, Log.class);

   @Override
   public Log getLog() {
      return log;
   }

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_21);
   }

   @Override
   public void writeClientListenerParams(ByteBuf buf, ClientListener clientListener, byte[][] filterFactoryParams, byte[][] converterFactoryParams) {
      super.writeClientListenerParams(buf, clientListener, filterFactoryParams, converterFactoryParams);
      buf.writeByte((short)(clientListener.useRawData() ? 1 : 0));
   }

   @Override
   protected ClientEvent readPartialEvent(ByteBuf buf, byte[] expectedListenerId, Marshaller marshaller, short eventTypeId, List<String> whitelist, SocketAddress serverAddress) {
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
         case CACHE_ENTRY_EXPIRED_EVENT_RESPONSE:
            eventType = ClientEvent.Type.CLIENT_CACHE_ENTRY_EXPIRED;
            break;
         case ERROR_RESPONSE:
            checkForErrorsInResponseStatus(buf, null, status, serverAddress);
         default:
            throw log.unknownEvent(eventTypeId);
      }

      byte[] listenerId = ByteBufUtil.readArray(buf);
      if (!Arrays.equals(listenerId, expectedListenerId))
         throw log.unexpectedListenerId(printArray(listenerId), printArray(expectedListenerId));

      short isCustom = buf.readUnsignedByte();
      boolean isRetried = buf.readUnsignedByte() == 1 ? true : false;

      if (isCustom == 1) {
         final Object eventData = MarshallerUtil.bytes2obj(marshaller, ByteBufUtil.readArray(buf), status, whitelist);
         return createCustomEvent(eventData, eventType, isRetried);
      } else if (isCustom == 2) { // New in 2.1, dealing with raw custom events
         return createCustomEvent(ByteBufUtil.readArray(buf), eventType, isRetried); // Raw data
      } else {
         switch (eventType) {
            case CLIENT_CACHE_ENTRY_CREATED:
               Object createdKey = MarshallerUtil.bytes2obj(marshaller, ByteBufUtil.readArray(buf), status, whitelist);
               long createdDataVersion = buf.readLong();
               return createCreatedEvent(createdKey, createdDataVersion, isRetried);
            case CLIENT_CACHE_ENTRY_MODIFIED:
               Object modifiedKey = MarshallerUtil.bytes2obj(marshaller, ByteBufUtil.readArray(buf), status, whitelist);
               long modifiedDataVersion = buf.readLong();
               return createModifiedEvent(modifiedKey, modifiedDataVersion, isRetried);
            case CLIENT_CACHE_ENTRY_REMOVED:
               Object removedKey = MarshallerUtil.bytes2obj(marshaller, ByteBufUtil.readArray(buf), status, whitelist);
               return createRemovedEvent(removedKey, isRetried);
            case CLIENT_CACHE_ENTRY_EXPIRED:
               Object expiredKey = MarshallerUtil.bytes2obj(marshaller, ByteBufUtil.readArray(buf), status, whitelist);
               return createExpiredEvent(expiredKey);
            default:
               throw getLog().unknownEvent(eventTypeId);
         }
      }
   }

   protected ClientEvent createExpiredEvent(final Object key) {
      return new ClientCacheEntryExpiredEvent() {
         @Override public Object getKey() { return key; }
         @Override public Type getType() { return Type.CLIENT_CACHE_ENTRY_EXPIRED; }
         @Override
         public String toString() {
            return "ClientCacheEntryExpiredEvent(" + "key=" + key + ")";
         }
      };
   }
}
