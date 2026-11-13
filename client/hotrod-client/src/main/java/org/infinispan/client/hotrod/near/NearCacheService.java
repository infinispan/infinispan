package org.infinispan.client.hotrod.near;

import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.BiConsumer;
import java.util.function.ToIntFunction;

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
import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.impl.MetadataValueImpl;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.BloomFilter;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.MurmurHash3BloomFilter;
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
   private volatile byte[] listenerId;
   private NearCache<K, V> cache;
   private Runnable invalidationCallback;
   private final int bloomFilterBits;
   private final int bloomFilterUpdateThreshold;
   private final AtomicInteger nearCacheRemovals;
   private InvalidatedNearRemoteCache<K, V> remote;

   private record SegmentState(ToIntFunction<Object> function, AtomicIntegerArray epochs) {}

   private Channel channelUsed;
   private volatile SegmentState segmentState;

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
         nearCacheRemovals = new AtomicInteger();
      } else {
         bloomFilterBits = -1;
         bloomFilterUpdateThreshold = -1;
         nearCacheRemovals = null;
      }
   }

   public Channel start(InvalidatedNearRemoteCache<K, V> remote) {
      if (cache == null) {
         // Create near cache
         cache = createNearCache(config, this::entryRemovedFromNearCache);
         // Add a listener that updates the near cache
         listener = new InvalidatedNearCacheListener<>(this);
         channelUsed = remote.addNearCacheListener(listener, bloomFilterBits);
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
      if (nearCacheRemovals == null) {
         return;
      }

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
      MetadataValue<V> valueToStore = newValue;
      if (newValue != null && newValue.getValue() != null) {
         // A read must retain the epoch captured by its placeholder, even if ownership changes
         // between response validation and this replacement.
         valueToStore = prevValue instanceof EpochMetadataValue<V> placeholder
               ? placeholder.withValue(newValue) : withCurrentEpoch(key, newValue);
      }
      boolean replaced = cache.replace(key, prevValue, valueToStore);
      if (log.isTraceEnabled()) {
         log.tracef("Replaced key=%s and value=%s with new value=%s in near cache (listenerId=%s): %s",
               key, prevValue, newValue, Util.printArray(listenerId), replaced);
      }
      return replaced;
   }

   /**
    * Creates a read placeholder carrying the current segment epoch, so an in-flight response
    * cannot be cached under a newer ownership generation.
    *
    * @param key the key being read
    * @return a unique placeholder for the pending read
    */
   public MetadataValue<V> createPlaceholder(K key) {
      return withCurrentEpoch(key, new MetadataValueImpl<>(-1, -1, -1, -1, -1, null));
   }

   private MetadataValue<V> withCurrentEpoch(K key, MetadataValue<V> value) {
      SegmentState state = segmentState;
      if (state != null) {
         int segment = state.function().applyAsInt(key);
         AtomicIntegerArray epochs = state.epochs();
         if (segment >= 0 && segment < epochs.length()) {
            return new EpochMetadataValue<>(value, epochs.get(segment), segment, epochs);
         }
      }
      return value;
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
         // There was a false positive, add that to the removal
         entryRemovedFromNearCache(key, null);
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
         if (value instanceof EpochMetadataValue<V> emv) {
            SegmentState state = segmentState;
            AtomicIntegerArray epochs = state != null ? state.epochs() : null;
            if (!emv.isCurrent(epochs)) {
               // The topology change already invalidated every older entry in this segment.
               // Advancing the epoch here would invalidate fresh entries as they are repopulated.
               cache.remove(key, value);
               return null;
            }
            value = emv.getDelegate();
         }
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
      if (nearCacheRemovals != null) {
         nearCacheRemovals.set(0);
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

   public boolean isConnected() {
      if (remote != null && remote.isPrimaryMode()) {
         return remote.hasActivePrimaryListeners();
      }
      return listenerNotifier.isListenerConnected(listenerId);
   }

   public void setInvalidationCallback(Runnable r) {
      this.invalidationCallback = r;
   }

   /**
    * Updates the listener ID tracked by this service. Used when the near cache switches between
    * primary-owner and cluster-wide listener mode without a full stop/start cycle.
    */
   public void updateListenerId(byte[] newListenerId) {
      this.listenerId = newListenerId;
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

   /**
    * Replaces the segment mapping and its epoch generation together. Values and pending read placeholders
    * carrying a previous generation are no longer current, even if the owners have not changed.
    *
    * @param segmentFunction maps keys to segments, or {@code null} to disable segment tracking
    * @param numSegments the number of segments in the new mapping
    */
   public void setSegmentFunction(ToIntFunction<Object> segmentFunction, int numSegments) {
      if (segmentFunction != null && numSegments > 0) {
         this.segmentState = new SegmentState(segmentFunction, new AtomicIntegerArray(numSegments));
      } else {
         this.segmentState = null;
      }
   }

   public void invalidateSegment(int segment) {
      SegmentState state = segmentState;
      if (state != null && state.epochs() != null && segment >= 0 && segment < state.epochs().length()) {
         state.epochs().incrementAndGet(segment);
         if (log.isTraceEnabled()) {
            log.tracef("Invalidated segment %d (new epoch %d) for near cache %s",
                  segment, state.epochs().get(segment), remote != null ? remote.getName() : "");
         }
      }
   }

   public void invalidateSegments(Collection<Integer> segments) {
      SegmentState state = segmentState;
      if (state != null && state.epochs() != null) {
         AtomicIntegerArray epochs = state.epochs();
         for (int s : segments) {
            if (s >= 0 && s < epochs.length()) {
               epochs.incrementAndGet(s);
               if (log.isTraceEnabled()) {
                  log.tracef("Invalidated segment %d (new epoch %d) for near cache %s",
                        s, epochs.get(s), remote != null ? remote.getName() : "");
               }
            }
         }
      }
   }

   public void invalidateSegments(BitSet segments) {
      SegmentState state = segmentState;
      if (state != null && state.epochs() != null) {
         for (int s = segments.nextSetBit(0); s >= 0; s = segments.nextSetBit(s + 1)) {
            invalidateSegment(s);
         }
      }
   }

   /** Fences every segment, when it isn't known which ones had their primary owner changed. */
   public void invalidateAllSegments() {
      SegmentState state = segmentState;
      if (state != null && state.epochs() != null) {
         for (int s = 0; s < state.epochs().length(); ++s) {
            invalidateSegment(s);
         }
      }
   }

   public byte[] calculateBloomBits() {
      if (bloomFilterBits <= 0) {
         return null;
      }
      BloomFilter<byte[]> bloomFilter = MurmurHash3BloomFilter.createFilter(bloomFilterBits);
      for (Map.Entry<K, MetadataValue<V>> entry : cache) {
         MetadataValue<V> value = entry.getValue();
         if (value instanceof EpochMetadataValue<V> emv) {
            SegmentState state = segmentState;
            if (!emv.isCurrent(state != null ? state.epochs() : null) && cache.remove(entry.getKey(), value)) {
               continue;
            }
         }
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
