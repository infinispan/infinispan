package org.infinispan.client.hotrod.near;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
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
import org.infinispan.commons.util.concurrent.CompletableFutures;

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
   /**
    * Upper bound for how many keys are sent in one removal message, so that a very large near cache does not turn
    * into a very large request.
    */
   private static final int MAX_BLOOM_REMOVAL_BATCH = 1024;

   private final int bloomFilterBits;
   private final int bloomFilterUpdateThreshold;
   private final int bloomRemovalBatchSize;
   private final AtomicInteger nearCacheRemovals;
   /**
    * Keys the server still has in its bloom filter but which are no longer worth invalidating. They are batched up
    * and sent to the server once {@link #bloomRemovalBatchSize} of them accumulated. Only used when the server
    * supports removing individual keys from the filter, {@code null} when the bloom filter is disabled.
    */
   private final List<byte[]> pendingBloomRemovals;
   private InternalRemoteCache<K, V> remote;

   private Channel channelUsed;

   protected NearCacheService(NearCacheConfiguration config, ClientListenerNotifier listenerNotifier) {
      this.config = config;
      this.listenerNotifier = listenerNotifier;

      int maxEntries = config.maxEntries();
      if (maxEntries > 0 && config.bloomFilter()) {
         bloomFilterBits = determineBloomFilterBits(maxEntries);
         // We want to scale the update frequency of the bloom filter to be based on the number of max entries
         // This number along with default values of 3 hash algorithms and 4x bit size we end up with
         // between 14.689 and 16.573 percent hits per entry.
         bloomFilterUpdateThreshold = maxEntries / 16 + 3;
         bloomRemovalBatchSize = Math.min(bloomFilterUpdateThreshold, MAX_BLOOM_REMOVAL_BATCH);
         nearCacheRemovals = new AtomicInteger();
         pendingBloomRemovals = new ArrayList<>(bloomRemovalBatchSize);
      } else {
         bloomFilterBits = -1;
         bloomFilterUpdateThreshold = -1;
         bloomRemovalBatchSize = -1;
         nearCacheRemovals = null;
         pendingBloomRemovals = null;
      }
   }

   public Channel start(InternalRemoteCache<K, V> remote) {
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
      this.remote = remote;
      return channelUsed;
   }

   private static int determineBloomFilterBits(int maxEntries) {
      int bloomFilterBitScaler = Integer.parseInt(System.getProperty("infinispan.bloom-filter.bit-multiplier", "4"));
      return maxEntries * bloomFilterBitScaler;
   }

   void entryRemovedFromNearCache(K key, MetadataValue<V> value) {
      if (nearCacheRemovals == null || remote == null) {
         return;
      }

      if (removeKeysFromBloomFilterSupported()) {
         // The placeholder that is inserted while a remote read is in flight holds a null value and was never added
         // to the server side filter by us, so it must not be removed from it either
         if (value != null && value.getValue() != null) {
            keyNoLongerInServerFilter(key);
         }
         return;
      }

      // The server is too old to remove individual keys, fall back to periodically recomputing the entire filter
      while (true) {
         int removals = nearCacheRemovals.get();
         if (removals >= bloomFilterUpdateThreshold) {
            if (nearCacheRemovals.compareAndSet(removals, 0)) {
               log.tracef("Updating bloom filter due to reaching update threshold with %d for %s", removals, remote.getName());
               remote.updateBloomFilter();
               break;
            }
         } else if (nearCacheRemovals.compareAndSet(removals, removals + 1)) {
            log.tracef("Incremented nearCacheRemovals to %d for %s", removals + 1, remote.getName());
            break;
         }
      }
   }

   /**
    * Invoked when a remote read added the key to the server side bloom filter but its result was not stored in the
    * near cache. Without this the filter would slowly fill up with keys that are never going to be invalidated,
    * which costs nothing in correctness but increasingly more in false positives.
    * <p>
    * The caller must only invoke this when the read actually reached the node holding the filter and did not overlap
    * a full filter update, otherwise the key may be removed from the filter more often than it was added.
    */
   public void entryNotCached(K key) {
      if (nearCacheRemovals == null || remote == null || !removeKeysFromBloomFilterSupported()) {
         return;
      }
      keyNoLongerInServerFilter(key);
   }

   /**
    * @return whether the node holding the bloom filter is able to remove individual keys from it. Servers older than
    * 16.3 can only be given a freshly computed filter in its entirety.
    */
   boolean removeKeysFromBloomFilterSupported() {
      return bloomFilterBits > 0 && remote.supportsBloomFilterKeyRemoval();
   }

   private void keyNoLongerInServerFilter(K key) {
      byte[] keyBytes = remote.keyToBytes(key);
      List<byte[]> batch;
      synchronized (pendingBloomRemovals) {
         pendingBloomRemovals.add(keyBytes);
         if (pendingBloomRemovals.size() < bloomRemovalBatchSize) {
            if (log.isTraceEnabled())
               log.tracef("Pending bloom filter removals is %d for %s", pendingBloomRemovals.size(), remote.getName());
            return;
         }
         batch = new ArrayList<>(pendingBloomRemovals);
         pendingBloomRemovals.clear();
      }
      sendBloomRemovals(batch);
   }

   /**
    * Sends whatever removals are still buffered. Invoked when the server tells us about a key we do not have, as
    * that is a strong hint that its filter is stale and that it is worth paying for the round trip right away.
    */
   private void flushPendingBloomRemovals() {
      List<byte[]> batch;
      synchronized (pendingBloomRemovals) {
         if (pendingBloomRemovals.isEmpty()) {
            return;
         }
         batch = new ArrayList<>(pendingBloomRemovals);
         pendingBloomRemovals.clear();
      }
      sendBloomRemovals(batch);
   }

   /**
    * Sends every removal that is still buffered to the server, even when there is nothing to send. Because all bloom
    * filter operations travel over the listener connection, waiting for the returned stage also guarantees that any
    * earlier removal batch was applied.
    *
    * @return stage that completes once the server side filter caught up with this near cache
    */
   public CompletionStage<Void> flushBloomFilterRemovals() {
      if (bloomFilterBits <= 0 || remote == null) {
         return CompletableFutures.completedNull();
      }
      if (!removeKeysFromBloomFilterSupported()) {
         // The server can only be given the entire filter, recompute it from what the near cache holds now
         nearCacheRemovals.set(0);
         return remote.updateBloomFilter();
      }
      List<byte[]> batch;
      synchronized (pendingBloomRemovals) {
         batch = new ArrayList<>(pendingBloomRemovals);
         pendingBloomRemovals.clear();
      }
      return remote.removeBloomFilterKeys(batch);
   }

   private void sendBloomRemovals(List<byte[]> batch) {
      log.tracef("Removing %d keys from the server bloom filter for %s", batch.size(), remote.getName());
      ignoreFailure(remote.removeBloomFilterKeys(batch));
   }

   /**
    * Bloom filter maintenance is best effort, a failure only means the server sends invalidations it could have
    * skipped, so it is logged instead of being propagated to the caller that triggered the near cache change.
    */
   private void ignoreFailure(CompletionStage<Void> stage) {
      stage.whenComplete((ignore, t) -> {
         if (t != null && log.isTraceEnabled()) {
            log.tracef(t, "Unable to update the server bloom filter for listenerId=%s", Util.printArray(listenerId));
         }
      });
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
      boolean inserted = cache.putIfAbsent(key, value);

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
         log.tracef("Received false positive remove for key=%s from near cache (listenedId=%s)", key, Util.printArray(listenerId));
         falsePositiveInvalidation(key);
      }

      return removed;
   }

   /**
    * The server invalidated a key we do not hold. The key itself must not be removed from the server filter, as its
    * bits may well belong to keys we do hold, but it does tell us the filter is behind, so we send whatever removals
    * are still buffered.
    */
   private void falsePositiveInvalidation(K key) {
      if (nearCacheRemovals == null || remote == null) {
         return;
      }
      if (removeKeysFromBloomFilterSupported()) {
         flushPendingBloomRemovals();
      } else {
         // Older servers can only be given the whole filter, so count towards the next full update
         entryRemovedFromNearCache(key, null);
      }
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
      ignoreFailure(clearAndResetBloomFilter());
   }

   /**
    * Empties the near cache and, when a bloom filter is in use, resets the one the server keeps for this client so
    * that it stops sending invalidations for entries we no longer hold. Reads that overlap the reset are not cached,
    * which is what the bloom filter version handling in
    * {@link org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache} takes care of.
    *
    * @return stage that completes once the server side filter was reset as well
    */
   public CompletionStage<Void> clearAndResetBloomFilter() {
      cache.clear();
      if (log.isTraceEnabled()) log.tracef("Cleared near cache (listenerId=%s)", Util.printArray(listenerId));
      if (nearCacheRemovals == null) {
         return CompletableFutures.completedNull();
      }
      nearCacheRemovals.set(0);
      synchronized (pendingBloomRemovals) {
         // The whole filter is about to be replaced, so anything we have not sent yet is meaningless
         pendingBloomRemovals.clear();
      }
      // The listener may already be gone, for example when clearing after a fail-over, in which case the server
      // discards the filter along with the connection anyway
      return remote != null ? remote.updateBloomFilter() : CompletableFutures.completedNull();
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
