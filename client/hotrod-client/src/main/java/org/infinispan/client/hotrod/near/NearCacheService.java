package org.infinispan.client.hotrod.near;

import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryExpired;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientCacheFailover;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.client.hotrod.event.ClientCacheEntryExpiredEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryModifiedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;
import org.infinispan.client.hotrod.event.ClientCacheFailoverEvent;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.BloomFilter;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.MurmurHash3BloomFilter;
import org.infinispan.commons.util.Util;

/**
 * Near cache service, manages the lifecycle of the near cache.
 *
 * @since 7.1
 */
public class NearCacheService<K, V> implements NearCache<K, V> {
   private static final Log log = LogFactory.getLog(NearCacheService.class);

   private final NearCacheConfiguration config;
   private final ClientListenerNotifier listenerNotifier;
   private final AtomicInteger nearCacheRemovals = new AtomicInteger();
   private Object listener;
   private byte[] listenerId;
   private NearCache<K, V> cache;
   private Runnable invalidationCallback;
   private int bloomFilterBits = -1;
   private int bloomFilterUpdateThreshold;
   private InternalRemoteCache<K, V> remote;

   private SocketAddress listenerAddress;

   protected NearCacheService(NearCacheConfiguration config, ClientListenerNotifier listenerNotifier) {
      this.config = config;
      this.listenerNotifier = listenerNotifier;
   }

   public SocketAddress start(InternalRemoteCache<K, V> remote) {
      if (cache == null) {
         // Create near cache
         cache = createNearCache(config, this::entryRemovedFromNearCache);
         // Add a listener that updates the near cache
         listener = new InvalidatedNearCacheListener<>(this);
         int maxEntries = config.maxEntries();
         if (maxEntries > 0 && config.bloomFilter()) {
            bloomFilterBits = determineBloomFilterBits(maxEntries);
            // We want to scale the update frequency of the bloom filter to be based on the number of max entries
            // This number along with default values of 3 hash algorithms and 4x bit size we end up with
            // between 14.689 and 16.573 percent hits per entry.
            bloomFilterUpdateThreshold = maxEntries / 16 + 3;
            listenerAddress = remote.addNearCacheListener(listener, bloomFilterBits);
         } else {
            remote.addClientListener(listener);
         }
         // Get the listener ID for faster listener connected lookups
         listenerId = listenerNotifier.findListenerId(listener);
      }
      this.remote = remote;
      return listenerAddress;
   }

   private static int determineBloomFilterBits(int maxEntries) {
      int bloomFilterBitScaler = Integer.parseInt(System.getProperty("infinispan.bloom-filter.bit-multiplier", "4"));
      return maxEntries * bloomFilterBitScaler;
   }

   void entryRemovedFromNearCache(K key, MetadataValue<V> value) {

      while (true) {
         int removals = nearCacheRemovals.get();
         if (removals >= bloomFilterUpdateThreshold) {
            if (nearCacheRemovals.compareAndSet(removals, 0)) {
               remote.updateBloomFilter();
               break;
            }
         } else if (nearCacheRemovals.compareAndSet(removals, removals + 1)) {
            break;
         }
      }
   }

   public void stop(RemoteCache<K, V> remote) {
      if (log.isTraceEnabled())
         log.tracef("Stop near cache, remove underlying listener id %s", Util.printArray(listenerId));

      // Remove listener
      remote.removeClientListener(listener);
      // Empty cache
      cache.clear();
   }

   protected NearCache<K, V> createNearCache(NearCacheConfiguration config, BiConsumer<K, MetadataValue<V>> removedConsumer) {
      return config.nearCacheFactory().createNearCache(config, removedConsumer);
   }

   public static <K, V> NearCacheService<K, V> create(
         NearCacheConfiguration config, ClientListenerNotifier listenerNotifier) {
      return new NearCacheService<>(config, listenerNotifier);
   }

   @Override
   public void put(K key, MetadataValue<V> value) {
       cache.put(key, value);

      if (log.isTraceEnabled())
         log.tracef("Put key=%s and value=%s in near cache (listenerId=%s)",
               key, value, Util.printArray(listenerId));
   }

   @Override
   public void putIfAbsent(K key, MetadataValue<V> value) {
      cache.putIfAbsent(key, value);

      if (log.isTraceEnabled())
         log.tracef("Conditionally put key=%s and value=%s if absent in near cache (listenerId=%s)",
               key, value, Util.printArray(listenerId));
   }

   @Override
   public boolean remove(K key) {
      boolean removed = cache.remove(key);
      if (removed) {
         if (invalidationCallback != null) {
            invalidationCallback.run();
         }
         if (log.isTraceEnabled())
            log.tracef("Removed key=%s from near cache (listenedId=%s)", key, Util.printArray(listenerId));
      } else {
         log.tracef("Received false positive remove for key=%s from near cache (listenedId=%s)", key, Util.printArray(listenerId));
         // There was a false positive, add that to the removal
         entryRemovedFromNearCache(key, null);
      }

      return removed;
   }

   @Override
   public MetadataValue<V> get(K key) {
      boolean listenerConnected = isConnected();
      if (listenerConnected) {
         MetadataValue<V> value = cache.get(key);
         if (log.isTraceEnabled())
            log.tracef("Get key=%s returns value=%s (listenerId=%s)", key, value, Util.printArray(listenerId));

         return value;
      }

      if (log.isTraceEnabled())
         log.tracef("Near cache disconnected from server, returning null for key=%s (listenedId=%s)",
               key, Util.printArray(listenerId));

      return null;
   }

   @Override
   public void clear() {
      cache.clear();
      if (log.isTraceEnabled()) log.tracef("Cleared near cache (listenerId=%s)", Util.printArray(listenerId));
   }

   @Override
   public int size() {
      return cache.size();
   }

   @Override
   public Iterator<Map.Entry<K, MetadataValue<V>>> iterator() {
      return cache.iterator();
   }

   boolean isConnected() {
      return listenerNotifier.isListenerConnected(listenerId);
   }

   public void setInvalidationCallback(Runnable r) {
      this.invalidationCallback = r;
   }

   public int getBloomFilterBits() {
      return bloomFilterBits;
   }

   public byte[] getListenerId() {
      return listenerId;
   }

   public byte[] calculateBloomBits() {
      if (bloomFilterBits <= 0) {
         return null;
      }
      BloomFilter<byte[]> bloomFilter = MurmurHash3BloomFilter.createFilter(bloomFilterBits);
      for (Map.Entry<K, MetadataValue<V>> entry : cache) {
         bloomFilter.addToFilter(remote.keyToBytes(entry.getKey()));
      }

      IntSet intSet = bloomFilter.getIntSet();
      return intSet.toBitSet();
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

      @ClientCacheEntryExpired
      @SuppressWarnings("unused")
      public void handleExpiredEvent(ClientCacheEntryExpiredEvent<K> event) {
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
}
