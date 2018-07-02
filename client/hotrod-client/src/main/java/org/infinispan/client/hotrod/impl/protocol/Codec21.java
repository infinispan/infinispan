package org.infinispan.client.hotrod.impl.protocol;

import java.net.SocketAddress;
import java.util.function.Function;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.event.impl.AbstractClientEvent;
import org.infinispan.client.hotrod.event.impl.ExpiredEventImpl;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.ClassWhiteList;

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
      buf.writeByte((short) (clientListener.useRawData() ? 1 : 0));
   }

   @Override
   public AbstractClientEvent readCacheEvent(ByteBuf buf, Function<byte[], DataFormat> listenerDataFormat, short eventTypeId, ClassWhiteList whitelist, SocketAddress serverAddress) {
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
      short isCustom = buf.readUnsignedByte();
      boolean isRetried = buf.readUnsignedByte() == 1;
      DataFormat dataFormat = listenerDataFormat.apply(listenerId);

      if (isCustom == 1) {
         final Object eventData = dataFormat.valueToObj(ByteBufUtil.readArray(buf), status, whitelist);
         return createCustomEvent(listenerId, eventData, eventType, isRetried);
      } else if (isCustom == 2) { // New in 2.1, dealing with raw custom events
         return createCustomEvent(listenerId, ByteBufUtil.readArray(buf), eventType, isRetried); // Raw data
      } else {
         switch (eventType) {
            case CLIENT_CACHE_ENTRY_CREATED:
               Object createdKey = dataFormat.keyToObj(ByteBufUtil.readArray(buf), status, whitelist);
               long createdDataVersion = buf.readLong();
               return createCreatedEvent(listenerId, createdKey, createdDataVersion, isRetried);
            case CLIENT_CACHE_ENTRY_MODIFIED:
               Object modifiedKey = dataFormat.keyToObj(ByteBufUtil.readArray(buf), status, whitelist);
               long modifiedDataVersion = buf.readLong();
               return createModifiedEvent(listenerId, modifiedKey, modifiedDataVersion, isRetried);
            case CLIENT_CACHE_ENTRY_REMOVED:
               Object removedKey = dataFormat.keyToObj(ByteBufUtil.readArray(buf), status, whitelist);
               return createRemovedEvent(listenerId, removedKey, isRetried);
            case CLIENT_CACHE_ENTRY_EXPIRED:
               Object expiredKey = dataFormat.keyToObj(ByteBufUtil.readArray(buf), status, whitelist);
               return createExpiredEvent(listenerId, expiredKey);
            default:
               throw getLog().unknownEvent(eventTypeId);
         }
      }
   }

   protected AbstractClientEvent createExpiredEvent(byte[] listenerId, final Object key) {
      return new ExpiredEventImpl<>(listenerId, key);
   }
}
