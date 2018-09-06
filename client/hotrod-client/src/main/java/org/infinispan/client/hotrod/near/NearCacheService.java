package org.infinispan.client.hotrod.near;

import java.nio.ByteBuffer;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientCacheFailover;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryModifiedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;
import org.infinispan.client.hotrod.event.ClientCacheFailoverEvent;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.MetadataValueImpl;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;

/**
 * Near cache service, manages the lifecycle of the near cache.
 *
 * @since 7.1
 */
public class NearCacheService<K, V> implements NearCache<K, V> {
   private static final Log log = LogFactory.getLog(NearCacheService.class);
   private static final boolean trace = log.isTraceEnabled();

   private final NearCacheConfiguration config;
   private final ClientListenerNotifier listenerNotifier;
   private Object listener;
   private byte[] listenerId;
   private NearCache<K, V> cache;
   private Runnable invalidationCallback;

   protected NearCacheService(NearCacheConfiguration config, ClientListenerNotifier listenerNotifier) {
      this.config = config;
      this.listenerNotifier = listenerNotifier;
   }

   public void start(RemoteCache<K, V> remote) {
      if (cache == null) {
         // Create near cache
         cache = createNearCache(config);
         // Add a listener that updates the near cache
         listener = createListener(remote);
         remote.addClientListener(listener);
         // Get the listener ID for faster listener connected lookups
         listenerId = listenerNotifier.findListenerId(listener);
      }
   }

   private Object createListener(RemoteCache<K, V> remote) {
      return config.mode().invalidated()
            ? new InvalidatedNearCacheListener(this)
            : new EagerNearCacheListener(this, remote.getRemoteCacheManager().getMarshaller());
   }

   public void stop(RemoteCache<K, V> remote) {
      if (trace)
         log.tracef("Stop near cache, remove underlying listener id %s", Util.printArray(listenerId));

      // Remove listener
      remote.removeClientListener(listener);
      // Empty cache
      cache.clear();
   }

   protected NearCache<K, V> createNearCache(NearCacheConfiguration config) {
      return config.maxEntries() > 0
            ? BoundedConcurrentMapNearCache.create(config)
            : ConcurrentMapNearCache.create();
   }

   public static <K, V> NearCacheService<K, V> create(
         NearCacheConfiguration config, ClientListenerNotifier listenerNotifier) {
      return new NearCacheService(config, listenerNotifier);
   }

   @Override
   public void put(K key, MetadataValue<V> value) {
       cache.put(key, value);

      if (trace)
         log.tracef("Put key=%s and value=%s in near cache (listenerId=%s)",
               key, value, Util.printArray(listenerId));
   }

   @Override
   public void putIfAbsent(K key, MetadataValue<V> value) {
      cache.putIfAbsent(key, value);

      if (trace)
         log.tracef("Conditionally put key=%s and value=%s if absent in near cache (listenerId=%s)",
               key, value, Util.printArray(listenerId));
   }

   @Override
   public boolean remove(K key) {
      boolean removed = cache.remove(key);
      if (removed && invalidationCallback != null) {
         invalidationCallback.run();
      }

      if (trace)
         log.tracef("Removed key=%s from near cache (listenedId=%s)", key, Util.printArray(listenerId));
      return removed;
   }

   @Override
   public MetadataValue<V> get(K key) {
      boolean listenerConnected = isConnected();
      if (listenerConnected) {
         MetadataValue<V> value = cache.get(key);
         if (trace)
            log.tracef("Get key=%s returns value=%s (listenerId=%s)", key, value, Util.printArray(listenerId));

         return value;
      }

      if (trace)
         log.tracef("Near cache disconnected from server, returning null for key=%s (listenedId=%s)",
               key, Util.printArray(listenerId));

      return null;
   }

   @Override
   public void clear() {
      cache.clear();
      if (trace) log.tracef("Cleared near cache (listenerId=%s)", Util.printArray(listenerId));
   }

   @Override
   public int size() {
      return cache.size();
   }

   private boolean isConnected() {
      return listenerNotifier.isListenerConnected(listenerId);
   }

   public void setInvalidationCallback(Runnable r) {
      this.invalidationCallback = r;
   }

   @ClientListener
   private static class InvalidatedNearCacheListener<K, V> {
      private static final Log log = LogFactory.getLog(InvalidatedNearCacheListener.class);
      private final NearCache<K, V> cache;

      private InvalidatedNearCacheListener(NearCache<K, V> cache) {
         this.cache = cache;
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
         if (trace) log.trace("Clear near cache after fail-over of server");
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
   @Deprecated
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
            MetadataValueImpl<V> entry = new MetadataValueImpl<>(0, 0, 0, 0, version, value);
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
            cache.remove(key);
         }
      }

      @ClientCacheFailover
      @SuppressWarnings("unused")
      public void handleFailover(ClientCacheFailoverEvent e) {
         if (trace) log.trace("Clear near cache after fail-over of server");
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
