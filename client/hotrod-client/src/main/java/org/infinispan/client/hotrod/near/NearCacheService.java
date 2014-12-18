package org.infinispan.client.hotrod.near;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientCacheFailover;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryModifiedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;
import org.infinispan.client.hotrod.event.ClientCacheFailoverEvent;
import org.infinispan.client.hotrod.event.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.VersionedValueImpl;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;

import java.nio.ByteBuffer;

public class NearCacheService<K, V> implements NearCache<K, V> {

   private final NearCacheConfiguration config;
   private final ClientListenerNotifier listenerNotifier;
   private Object listener;
   private NearCache<K, V> cache;

   protected NearCacheService(NearCacheConfiguration config, ClientListenerNotifier listenerNotifier) {
      this.config = config;
      this.listenerNotifier = listenerNotifier;
   }

   public void start(RemoteCache<K, V> remote) {
      // Create near cache
      cache = createNearCache(config);
      // Add a listener that updates the near cache
      listener = createListener(remote);
      remote.addClientListener(listener);
   }

   private Object createListener(RemoteCache<K, V> remote) {
      return config.mode().eager()
            ? new EagerNearCacheListener<K, V>(cache, remote.getRemoteCacheManager().getMarshaller())
            : new LazyNearCacheListener<K, V>(cache);
   }

   public void stop(RemoteCache<K, V> remote) {
      // Remove listener
      remote.removeClientListener(listener);
      // Empty cache
      cache.clear();
   }

   protected NearCache<K, V> createNearCache(NearCacheConfiguration config) {
      return config.maxEntries() > 0
            ? LinkedMapNearCache.<K, V>create(config)
            : ConcurrentMapNearCache.<K, V>create();
   }

   public static <K, V> NearCacheService<K, V> create(NearCacheConfiguration config, ClientListenerNotifier listenerNotifier) {
      return new NearCacheService<K, V>(config, listenerNotifier);
   }

   @Override
   public void put(K key, VersionedValue<V> value) {
      cache.put(key, value);
   }

   @Override
   public void putIfAbsent(K key, VersionedValue<V> value) {
      cache.putIfAbsent(key, value);
   }

   @Override
   public void remove(K key) {
      cache.remove(key);
   }

   @Override
   public VersionedValue<V> get(K key) {
      return isConnected() ? cache.get(key) : null;
   }

   @Override
   public void clear() {
      cache.clear();
   }

   private boolean isConnected() {
      return listenerNotifier.isListenerConnected(listener);
   }

   @ClientListener
   private static class LazyNearCacheListener<K, V> {
      private static final Log log = LogFactory.getLog(LazyNearCacheListener.class);
      private final NearCache<K, V> cache;

      private LazyNearCacheListener(NearCache<K, V> cache) {
         this.cache = cache;
      }

      @ClientCacheEntryCreated
      @SuppressWarnings("unused")
      public void handleCreatedEvent(ClientCacheEntryCreatedEvent<K> event) {
         invalidate(event.getKey());
      }

      @ClientCacheEntryModified
      @SuppressWarnings("unused")
      public void handleModifiedEvent(ClientCacheEntryModifiedEvent<K> event) {
         invalidate(event.getKey());
      }

      @ClientCacheEntryRemoved
      @SuppressWarnings("unused")
      public void handleRemovedEvent(ClientCacheEntryRemovedEvent<K> event) {
         invalidate(event.getKey());
      }

      @ClientCacheFailover
      @SuppressWarnings("unused")
      public void handleFailover(ClientCacheFailoverEvent e) {
         if (log.isTraceEnabled()) log.trace("Clear near cache after fail-over of server");
         cache.clear();
      }


      private void invalidate(K key) {
         cache.remove(key);
      }
   }

   /**
    * An near cache listener that eagerly populates the near cache as cache
    * entries are created/modified in the server. It uses a converter in order
    * to ship value and version information as well as the key. To avoid sharing
    * classes between client and server, it uses raw data in the converter.
    * This listener does not apply any filtering, so all keys are considered.
    */
   @ClientListener(converterFactoryName = "___eager-key-value-version-converter", useRawData = true)
   private static class EagerNearCacheListener<K, V> {
      private static final Log log = LogFactory.getLog(EagerNearCacheListener.class);
      private final NearCache<K, V> cache;
      private final Marshaller marshaller;

      private EagerNearCacheListener(NearCache<K, V> cache, Marshaller marshaller) {
         this.cache = cache;
         this.marshaller = marshaller;
      }

      @ClientCacheEntryCreated
      @ClientCacheEntryModified
      @SuppressWarnings("unused")
      public void handleCreatedModifiedEvent(ClientCacheEntryCustomEvent<byte[]> e) {
         ByteBuffer in = ByteBuffer.wrap(e.getEventData());
         byte[] keyBytes = extractElement(in);
         byte[] valueBytes = extractElement(in);
         K key = unmarshallObject(keyBytes, "key");
         V value = unmarshallObject(valueBytes, "value");
         long version = in.getLong();
         if (key != null && value != null) {
            VersionedValueImpl<V> entry = new VersionedValueImpl<>(version, value);
            if (log.isTraceEnabled()) log.tracef("Store key=%s and value=%s in near cache", key, entry);
            cache.put(key, entry);
         }
      }

      @SuppressWarnings("unchecked")
      private <T> T unmarshallObject(byte[] bytes, String element) {
         try {
            return (T) marshaller.objectFromByteBuffer(bytes);
         } catch (Exception e) {
            log.unableToUnmarshallBytesError(element, Util.toStr(bytes), e);
            return null;
         }
      }

      @ClientCacheEntryRemoved
      @SuppressWarnings("unused")
      public void handleRemovedEvent(ClientCacheEntryCustomEvent<byte[]> e) {
         ByteBuffer in = ByteBuffer.wrap(e.getEventData());
         byte[] keyBytes = extractElement(in);
         K key = unmarshallObject(keyBytes, "key");
         if (key != null) {
            if (log.isTraceEnabled()) log.tracef("Remove key=%s from near cache", key);
            cache.remove(key);
         }
      }

      @ClientCacheFailover
      @SuppressWarnings("unused")
      public void handleFailover(ClientCacheFailoverEvent e) {
         if (log.isTraceEnabled()) log.trace("Clear near cache after fail-over of server");
         cache.clear();
      }

      private static byte[] extractElement(ByteBuffer in) {
         int length = UnsignedNumeric.readUnsignedInt(in);
         byte[] element = new byte[length];
         in.get(element);
         return element;
      }
   }

}
