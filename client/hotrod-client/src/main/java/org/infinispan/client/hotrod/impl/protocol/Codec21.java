package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryExpiredEvent;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.commons.marshall.Marshaller;

import java.util.Arrays;
import java.util.List;

import static org.infinispan.commons.util.Util.printArray;

public class Codec21 extends Codec20 {

   private static final Log log = LogFactory.getLog(Codec21.class, Log.class);

   @Override
   public Log getLog() {
      return log;
   }

   @Override
   public HeaderParams writeHeader(Transport transport, HeaderParams params) {
      return writeHeader(transport, params, HotRodConstants.VERSION_21);
   }

   @Override
   public void writeClientListenerParams(Transport transport, ClientListener clientListener, byte[][] filterFactoryParams, byte[][] converterFactoryParams) {
      super.writeClientListenerParams(transport, clientListener, filterFactoryParams, converterFactoryParams);
      transport.writeByte((short)(clientListener.useRawData() ? 1 : 0));
   }

   @Override
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
         case CACHE_ENTRY_EXPIRED_EVENT_RESPONSE:
            eventType = ClientEvent.Type.CLIENT_CACHE_ENTRY_EXPIRED;
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
      boolean isRetried = transport.readByte() == 1 ? true : false;

      if (isCustom == 1) {
         final Object eventData = MarshallerUtil.bytes2obj(marshaller, transport.readArray(), status, whitelist);
         return createCustomEvent(eventData, eventType, isRetried);
      } else if (isCustom == 2) { // New in 2.1, dealing with raw custom events
         return createCustomEvent(transport.readArray(), eventType, isRetried); // Raw data
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
            case CLIENT_CACHE_ENTRY_EXPIRED:
               Object expiredKey = MarshallerUtil.bytes2obj(marshaller, transport.readArray(), status, whitelist);
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
