package org.infinispan.spring.remote.session;

import java.nio.ByteBuffer;
import java.util.Collections;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryExpired;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.common.session.AbstractApplicationPublisherBridge;
import org.infinispan.util.KeyValuePair;
import org.springframework.session.MapSession;
import org.springframework.session.Session;

/**
 * A bridge between Infinispan Remote events and Spring.
 *
 * @author Sebastian Łaskawiec
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.0
 */
@ClientListener(converterFactoryName = "___eager-key-value-version-converter", useRawData = true)
public class RemoteApplicationPublishedBridge extends AbstractApplicationPublisherBridge {

   private final DataFormat dataFormat;

   private final ClassAllowList allowList = new ClassAllowList(Collections.singletonList(".*"));

   public RemoteApplicationPublishedBridge(SpringCache eventSource) {
      super(eventSource);
      this.dataFormat = ((RemoteCache) eventSource.getNativeCache()).getDataFormat();
   }

   @Override
   protected void registerListener() {
      ((RemoteCache<?, ?>) eventSource.getNativeCache()).addClientListener(this, null, new Object[]{Boolean.TRUE});
   }

   @Override
   public void unregisterListener() {
      ((RemoteCache<?, ?>) eventSource.getNativeCache()).removeClientListener(this);
   }

   @ClientCacheEntryCreated
   public void processCacheEntryCreated(ClientCacheEntryCustomEvent<byte[]> event) {
      emitSessionCreatedEvent(readEvent(event).getValue());
   }

   @ClientCacheEntryExpired
   public void processCacheEntryExpired(ClientCacheEntryCustomEvent<byte[]> event) {
      emitSessionExpiredEvent(readEvent(event).getValue());
   }

   @ClientCacheEntryRemoved
   public void processCacheEntryDestroyed(ClientCacheEntryCustomEvent<byte[]> event) {
      emitSessionDestroyedEvent(readEvent(event).getValue());
   }

   protected KeyValuePair<String, Session> readEvent(ClientCacheEntryCustomEvent<byte[]> event) {
      byte[] eventData = event.getEventData();
      ByteBuffer rawData = ByteBuffer.wrap(eventData);
      byte[] rawKey = readElement(rawData);
      byte[] rawValue = readElement(rawData);
      String key = dataFormat.keyToObj(rawKey, allowList);
      KeyValuePair keyValuePair;
      if (rawValue == null) {
         // This events will hold either an old or a new value almost every time. But there are some corner cases
         // during rebalance where neither a new or an old value will be present. This if handles this case
         keyValuePair = new KeyValuePair<>(key, new MapSession(key));
      } else {
         keyValuePair = new KeyValuePair<>(key, dataFormat.valueToObj(rawValue, allowList));
      }
      return keyValuePair;
   }

   private byte[] readElement(ByteBuffer buffer) {
      if (buffer.remaining() == 0)
         return null;

      int length = UnsignedNumeric.readUnsignedInt(buffer);
      byte[] element = new byte[length];
      buffer.get(element);
      return element;
   }
}
