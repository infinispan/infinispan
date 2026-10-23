package org.infinispan.client.hotrod.near;

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
import org.infinispan.commons.util.CountingBloomFilter;
import org.infinispan.commons.util.MurmurHash3CountingBloomFilter;
import org.infinispan.commons.util.Util;

import io.netty.channel.Channel;

/**
 * Near cache service, manages the lifecycle of the near cache.
 *
 * @since 7.1
 */
public class NearCacheService<K, V> implements NearCache<K, V> {
   private static final Log log = LogFactory.getLog(NearCacheService.class);

   private final NearCacheConfiguration config;
   private final ClientListenerNotifier listenerNotifier;
   private Object listener;
   private byte[] listenerId;
   private NearCache<K, V> cache;
   private Runnable invalidationCallback;
   private final int bloomFilterBits;
   private final int bloomFilterUpdateThreshold;
   // Mirrors the keys held in the near cache, so the bits to send to the server are known without having to walk
   // the near cache and hash every key again. Null when the bloom filter is not in use.
   private final CountingBloomFilter<byte[]> bloomFilter;
   // Bits the near cache no longer needs but the server still has set, that is how stale the server filter is
   private final AtomicInteger staleBloomFilterBits;
   private InternalRemoteCache<K, V> remote;

   private Channel channelUsed;

   protected NearCacheService(NearCacheConfiguration config, ClientListenerNotifier listenerNotifier) {
      this.config = config;
      this.listenerNotifier = listenerNotifier;

      int maxEntries = config.maxEntries();
      int bits = maxEntries > 0 && config.bloomFilter() ? determineBloomFilterBits(maxEntries) : -1;
      if (bits > 0) {
         bloomFilterBits = bits;
         // We want to scale the update frequency of the bloom filter to be based on the number of max entries
         // This number along with default values of 3 hash algorithms and 4x bit size we end up with
         // between 14.689 and 16.573 percent hits per entry.
         // The threshold counts the bits a removal actually frees in the filter, as a removal which frees no bit
         // leaves the server with the very same filter it already has.
         bloomFilterUpdateThreshold = maxEntries / 16 + 3;
         bloomFilter = MurmurHash3CountingBloomFilter.createFilter(bloomFilterBits);
         staleBloomFilterBits = new AtomicInteger();
      } else {
         bloomFilterBits = -1;
         bloomFilterUpdateThreshold = -1;
         bloomFilter = null;
         staleBloomFilterBits = null;
      }
   }

   public Channel start(InternalRemoteCache<K, V> remote) {
      // Has to be set before the near cache exists, the bloom filter needs it to marshall the keys
      this.remote = remote;
      if (cache == null) {
         // Create near cache
         cache = createNearCache(config, this::entryRemovedFromNearCache);
         // Add a listener that updates the near cache
         listener = new InvalidatedNearCacheListener<>(this);
         if (bloomFilterBits > 0) {
            channelUsed = remote.addNearCacheListener(listener, bloomFilterBits);
         } else {
            remote.addClientListener(listener);
         }
         // Get the listener ID for faster listener connected lookups
         listenerId = listenerNotifier.findListenerId(listener);
      }
      return channelUsed;
   }

   private static int determineBloomFilterBits(int maxEntries) {
      int bloomFilterBitScaler = Integer.parseInt(System.getProperty("infinispan.bloom-filter.bit-multiplier", "4"));
      return maxEntries * bloomFilterBitScaler;
   }

   void entryRemovedFromNearCache(K key, MetadataValue<V> value) {
      removeFromBloomFilter(key);
   }

   /**
    * Adds the key to the client side bloom filter, if one is in use.
    * <p>
    * This has to be done before the key is visible in the near cache. A concurrent
    * {@link #calculateBloomBits()} that does not see the key would otherwise install a filter on the server which
    * does not match it, and the near cache would never be told that the entry changed.
    *
    * @return whether the key was added to the filter
    */
   private boolean addToBloomFilter(K key) {
      if (bloomFilter == null) {
         return false;
      }
      bloomFilter.addToFilter(remote.keyToBytes(key));
      return true;
   }

   /**
    * Removes a key that is no longer in the near cache from the client side bloom filter and asks the server for an
    * update once enough of its bits have become stale.
    */
   private void removeFromBloomFilter(K key) {
      if (bloomFilter == null) {
         return;
      }

      int clearedBits = bloomFilter.removeFromFilter(remote.keyToBytes(key));
      if (clearedBits == 0) {
         // The keys still in the near cache need every bit this key was using, the server filter is unaffected
         log.tracef("Removal of key=%s did not clear any bloom filter bit for %s", key, remote.getName());
         return;
      }

      while (true) {
         int staleBits = staleBloomFilterBits.get();
         int newStaleBits = staleBits + clearedBits;
         if (newStaleBits >= bloomFilterUpdateThreshold) {
            if (staleBloomFilterBits.compareAndSet(staleBits, 0)) {
               log.tracef("Updating bloom filter due to reaching update threshold with %d stale bits for %s",
                     newStaleBits, remote.getName());
               remote.updateBloomFilter();
               return;
            }
         } else if (staleBloomFilterBits.compareAndSet(staleBits, newStaleBits)) {
            log.tracef("Incremented stale bloom filter bits to %d for %s", newStaleBits, remote.getName());
            return;
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
   public boolean replace(K key, MetadataValue<V> prevValue, MetadataValue<V> newValue) {
      boolean replaced = cache.replace(key, prevValue, newValue);
      if (log.isTraceEnabled()) {
         log.tracef("Replaced key=%s and value=%s with new value=%s in near cache (listenerId=%s): %s",
               key, prevValue, newValue, Util.printArray(listenerId), replaced);
      }
      return replaced;
   }

   @Override
   public boolean putIfAbsent(K key, MetadataValue<V> value) {
      boolean addedToFilter = addToBloomFilter(key);
      boolean inserted = cache.putIfAbsent(key, value);
      if (addedToFilter && !inserted) {
         // Another entry for this key is already in the near cache and is already accounted for in the filter
         removeFromBloomFilter(key);
      }

      if (log.isTraceEnabled())
         log.tracef("Conditionally put %s if absent in near cache (listenerId=%s): %s", value,
               Util.printArray(listenerId), inserted);
      return inserted;
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
         // A false positive leaves the near cache, and so the bits the server needs, exactly as they were. Sending
         // the filter again would not drop a single bit, so it must not count towards the update threshold.
         log.tracef("Received false positive remove for key=%s from near cache (listenedId=%s)", key, Util.printArray(listenerId));
      }

      return removed;
   }

   @Override
   public boolean remove(K key, MetadataValue<V> value) {
      boolean removed = cache.remove(key, value);
      if (log.isTraceEnabled())
         log.tracef("Removed value=%s for key=%s from near cache (listenedId=%s): %s", value, key, Util.printArray(listenerId), removed);
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
      // The near cache notifies the removal of every entry it drops, which is what empties the bloom filter
      cache.clear();
      if (staleBloomFilterBits != null) {
         staleBloomFilterBits.set(0);
      }
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

   public NearCacheConfiguration getConfig() {
      return config;
   }

   public byte[] getListenerId() {
      return listenerId;
   }

   public byte[] calculateBloomBits() {
      if (bloomFilter == null) {
         return null;
      }
      // Whatever the server has is about to be replaced with the bits below, so nothing is stale anymore. Resetting
      // before taking the bits means a concurrent removal is counted twice at worst, never missed.
      staleBloomFilterBits.set(0);
      // The filter is kept up to date as entries come and go, so there is no need to walk the near cache
      return bloomFilter.toBitSet();
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
