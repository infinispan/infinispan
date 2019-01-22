package org.infinispan.api.reactive.client.impl.listener;

import java.nio.ByteBuffer;
import java.util.Collections;

import org.infinispan.api.client.listener.ClientKeyValueStoreListener;
import org.infinispan.api.reactive.EntryStatus;
import org.infinispan.api.reactive.KeyValueEntry;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent;
import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.io.UnsignedNumeric;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.reactivex.processors.FlowableProcessor;
import io.reactivex.processors.UnicastProcessor;

public class ClientListenerImpl<K, V> implements Publisher {
   private RemoteCache<Object, Object> cache;
   private final ClientKeyValueStoreListener listener;
   private final ClassWhiteList whitelist = new ClassWhiteList(Collections.singletonList(".*"));

   public ClientListenerImpl(RemoteCache cache, ClientKeyValueStoreListener listener) {
      this.cache = cache;
      this.listener = listener;
   }

   @Override
   public void subscribe(Subscriber subscriber) {
      UnicastProcessor processor = UnicastProcessor.create();
      RemoteClientListener listener = new RemoteClientListener(processor);
      processor.doOnError(e -> cache.removeClientListener(listener));
      processor.doOnCancel(() -> cache.removeClientListener(listener));
      processor.subscribe(subscriber);
      cache.addClientListener(listener);
   }

   @ClientListener(converterFactoryName = "___eager-key-value-version-converter",
         useRawData = true,
         includeCurrentState = true)
   class RemoteClientListener {
      private FlowableProcessor processor;

      public RemoteClientListener(FlowableProcessor processor) {
         this.processor = processor;
      }

      @ClientCacheEntryCreated
      public void handleCreated(ClientCacheEntryCustomEvent<byte[]> event) {
         if (listener.isListenCreated()) {
            KeyValueEntry<K, V> keyValueEntry = readEvent(event, EntryStatus.CREATED);
            processor.onNext(keyValueEntry);
         }
      }

      @ClientCacheEntryModified
      public void handleModified(ClientCacheEntryCustomEvent<byte[]> event) {
         if (listener.isListenUpdated()) {
            KeyValueEntry<K, V> keyValueEntry = readEvent(event, EntryStatus.UPDATED);
            processor.onNext(keyValueEntry);
         }
      }

      @ClientCacheEntryRemoved
      public void handleRemoved(ClientCacheEntryCustomEvent<byte[]> event) {
         if (listener.isListenDeleted()) {
            KeyValueEntry<K, V> keyValueEntry = readEvent(event, EntryStatus.DELETED);
            processor.onNext(keyValueEntry);
         }
      }
   }

   protected KeyValueEntry<K, V> readEvent(ClientCacheEntryCustomEvent<byte[]> event, EntryStatus entryStatus) {
      byte[] eventData = event.getEventData();
      ByteBuffer rawData = ByteBuffer.wrap(eventData);
      byte[] rawKey = readElement(rawData);
      byte[] rawValue = readElement(rawData);
      K key = cache.getDataFormat().keyToObj(rawKey, whitelist);
      KeyValueEntry keyValuePair;
      V value;
      if (rawValue != null) {
         // This events will hold either an old or a new value almost every time. But there are some corner cases
         // during rebalance where neither a new or an old value will be present. This if handles this case
         value = cache.getDataFormat().valueToObj(rawValue, whitelist);
         keyValuePair = new KeyValueEntry<>(key, value, entryStatus);
      } else {
         keyValuePair = new KeyValueEntry(key, null, entryStatus);
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
