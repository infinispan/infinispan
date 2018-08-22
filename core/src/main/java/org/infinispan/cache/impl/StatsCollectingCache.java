package org.infinispan.cache.impl;

import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.ByRef;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.Metadata;
import org.infinispan.stats.Stats;
import org.infinispan.stats.impl.StatsCollector;
import org.infinispan.stats.impl.StatsImpl;
import org.infinispan.commons.time.TimeService;

/**
 * Wraps existing {@link AdvancedCache} to collect statistics
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class StatsCollectingCache<K, V> extends SimpleCacheImpl<K, V> {
   @Inject private StatsCollector statsCollector;
   @Inject private TimeService timeService;

   public StatsCollectingCache(String cacheName) {
      super(cacheName);
   }

   public StatsCollectingCache(String cacheName, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      super(cacheName, keyDataConversion, valueDataConversion);
   }

   @Override
   public AdvancedCache<K, V> withFlags(Flag... flags) {
      return this;
   }

   @Override
   public AdvancedCache<K, V> with(ClassLoader classLoader) {
      return this;
   }

   @Override
   public Stats getStats() {
      return new StatsImpl(statsCollector.getStatisticsEnabled() ? statsCollector : null);
   }

   @Override
   public V get(Object key) {
      boolean statisticsEnabled = statsCollector.getStatisticsEnabled();
      long start = 0;
      if (statisticsEnabled) {
         start = timeService.time();
      }
      V value = super.get(key);
      if (statisticsEnabled) {
         long end = timeService.time();
         if (value == null) {
            statsCollector.recordMisses(1, end - start);
         } else {
            statsCollector.recordHits(1, end - start);
         }
      }
      return value;
   }

   @Override
   public CacheEntry<K, V> getCacheEntry(Object k) {
      boolean statisticsEnabled = statsCollector.getStatisticsEnabled();
      long start = 0;
      if (statisticsEnabled) {
         start = timeService.time();
      }
      CacheEntry<K, V> entry = super.getCacheEntry(k);
      if (statisticsEnabled) {
         long end = timeService.time();
         if (entry == null) {
            statsCollector.recordMisses(1, end - start);
         } else {
            statsCollector.recordHits(1, end - start);
         }
      }
      return entry;
   }

   @Override
   public Map<K, V> getAll(Set<?> keys) {
      boolean statisticsEnabled = statsCollector.getStatisticsEnabled();
      long start = 0;
      if (statisticsEnabled) {
         start = timeService.time();
      }
      Map<K, V> map = super.getAll(keys);
      if (statisticsEnabled) {
         long end = timeService.time();
         int requests = keys.size();
         int hits = 0;
         for (V value : map.values()) {
            if (value != null) hits++;
         }
         int misses = requests - hits;
         if (hits > 0) {
            statsCollector.recordHits(hits, hits * (end - start) / requests);
         }
         if (misses > 0) {
            statsCollector.recordMisses(misses, misses * (end - start) / requests);
         }
      }
      return map;
   }

   @Override
   public Map<K, CacheEntry<K, V>> getAllCacheEntries(Set<?> keys) {
      boolean statisticsEnabled = statsCollector.getStatisticsEnabled();
      long start = 0;
      if (statisticsEnabled) {
         start = timeService.time();
      }
      Map<K, CacheEntry<K, V>> map = super.getAllCacheEntries(keys);
      if (statisticsEnabled) {
         long end = timeService.time();
         int requests = keys.size();
         int hits = 0;
         for (CacheEntry<K, V> entry : map.values()) {
            if (entry != null && entry.getValue() != null) hits++;
         }
         int misses = requests - hits;
         if (hits > 0) {
            statsCollector.recordHits(hits, hits * (end - start) / requests);
         }
         if (misses > 0) {
            statsCollector.recordMisses(misses, misses * (end - start) / requests);
         }
      }
      return map;
   }

   @Override
   public Map<K, V> getAndPutAll(Map<? extends K, ? extends V> entries) {
      boolean statisticsEnabled = statsCollector.getStatisticsEnabled();
      long start = 0;
      if (statisticsEnabled) {
         start = timeService.time();
      }
      Map<K, V> map = super.getAndPutAll(entries);
      if (statisticsEnabled) {
         long end = timeService.time();
         statsCollector.recordStores(entries.size(), end - start);
      }
      return map;
   }

   @Override
   public void evict(K key) {
      super.evict(key);
      statsCollector.recordEviction();
   }

   @Override
   protected V getAndPutInternal(K key, V value, Metadata metadata) {
      boolean statisticsEnabled = statsCollector.getStatisticsEnabled();
      long start = 0;
      if (statisticsEnabled) {
         start = timeService.time();
      }
      V ret = super.getAndPutInternal(key, value, metadata);
      if (statisticsEnabled) {
         long end = timeService.time();
         statsCollector.recordStores(1, end - start);
      }
      return ret;
   }

   @Override
   protected V getAndReplaceInternal(K key, V value, Metadata metadata) {
      boolean statisticsEnabled = statsCollector.getStatisticsEnabled();
      long start = 0;
      if (statisticsEnabled) {
         start = timeService.time();
      }
      V ret = super.getAndReplaceInternal(key, value, metadata);
      if (statisticsEnabled && ret != null) {
         long end = timeService.time();
         statsCollector.recordStores(1, end - start);
      }
      return ret;
   }

   @Override
   protected void putForExternalReadInternal(K key, V value, Metadata metadata, ByRef.Boolean isCreatedRef) {
      boolean statisticsEnabled = statsCollector.getStatisticsEnabled();
      long start = 0;
      if (statisticsEnabled) {
         start = timeService.time();
      }
      super.putForExternalReadInternal(key, value, metadata, isCreatedRef);
      if (statisticsEnabled && isCreatedRef.get()) {
         long end = timeService.time();
         statsCollector.recordStores(1, end - start);
      }
   }

   @Override
   protected V putIfAbsentInternal(K key, V value, Metadata metadata) {
      boolean statisticsEnabled = statsCollector.getStatisticsEnabled();
      long start = 0;
      if (statisticsEnabled) {
         start = timeService.time();
      }
      V ret = super.putIfAbsentInternal(key, value, metadata);
      if (statisticsEnabled && ret == null) {
         long end = timeService.time();
         statsCollector.recordStores(1, end - start);
      }
      return ret;
   }

   @Override
   public V remove(Object key) {
      boolean statisticsEnabled = statsCollector.getStatisticsEnabled();
      long start = 0;
      if (statisticsEnabled) {
         start = timeService.time();
      }
      V ret = super.remove(key);
      if (statisticsEnabled) {
         long end = timeService.time();
         if (ret != null) {
            statsCollector.recordRemoveHits(1, end - start);
         } else {
            statsCollector.recordRemoveMisses(1);
         }
      }
      return ret;
   }

   @Override
   public boolean remove(Object key, Object value) {
      boolean statisticsEnabled = statsCollector.getStatisticsEnabled();
      long start = 0;
      if (statisticsEnabled) {
         start = timeService.time();
      }
      boolean removed = super.remove(key, value);
      if (statisticsEnabled) {
         long end = timeService.time();
         if (removed) {
            statsCollector.recordRemoveHits(1, end - start);
         } else {
            statsCollector.recordRemoveMisses(1);
         }
      }
      return removed;
   }

   @Override
   protected boolean replaceInternal(K key, V oldValue, V value, Metadata metadata) {
      boolean statisticsEnabled = statsCollector.getStatisticsEnabled();
      long start = 0;
      if (statisticsEnabled) {
         start = timeService.time();
      }
      boolean replaced = super.replaceInternal(key, oldValue, value, metadata);
      if (statisticsEnabled && replaced) {
         long end = timeService.time();
         statsCollector.recordStores(1, end - start);
      }
      return replaced;
   }

   @Override
   protected V computeIfAbsentInternal(K key, Function<? super K, ? extends V> mappingFunction, ByRef<V> newValueRef) {
      boolean statisticsEnabled = statsCollector.getStatisticsEnabled();
      long start = 0;
      if (statisticsEnabled) {
         start = timeService.time();
      }
      V ret = super.computeIfAbsentInternal(key, mappingFunction, newValueRef);
      if (statisticsEnabled) {
         long end = timeService.time();
         if (newValueRef.get() != null) {
            statsCollector.recordStores(1, end - start);
         }
      }
      return ret;
   }

   @Override
   protected V computeIfPresentInternal(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, CacheEntryChange<K, V> ref) {
      boolean statisticsEnabled = statsCollector.getStatisticsEnabled();
      long start = 0;
      if (statisticsEnabled) {
         start = timeService.time();
      }
      V ret = super.computeIfPresentInternal(key, remappingFunction, ref);
      if (statisticsEnabled) {
         long end = timeService.time();
         if (ref.getNewValue() != null) {
            statsCollector.recordStores(1, end - start);
         } else if (ref.getKey() != null) {
            statsCollector.recordRemoveHits(1, end - start);
         }
      }
      return ret;
   }

   @Override
   protected V computeInternal(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, CacheEntryChange<K, V> ref) {
      boolean statisticsEnabled = statsCollector.getStatisticsEnabled();
      long start = 0;
      if (statisticsEnabled) {
         start = timeService.time();
      }
      V ret = super.computeInternal(key, remappingFunction, ref);
      if (statisticsEnabled) {
         long end = timeService.time();
         if (ref.getNewValue() != null) {
            statsCollector.recordStores(1, end - start);
         } else if (ref.getKey() != null) {
            statsCollector.recordRemoveHits(1, end - start);
         }
      }
      return ret;
   }

   @Override
   protected V mergeInternal(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, CacheEntryChange<K, V> ref, Metadata metadata) {
      boolean statisticsEnabled = statsCollector.getStatisticsEnabled();
      long start = 0;
      if (statisticsEnabled) {
         start = timeService.time();
      }
      V ret = super.mergeInternal(key, value, remappingFunction, ref, metadata);
      if (statisticsEnabled) {
         long end = timeService.time();
         if (ref.getNewValue() != null) {
            statsCollector.recordStores(1, end - start);
         } else if (ref.getKey() != null) {
            statsCollector.recordRemoveHits(1, end - start);
         }
      }
      return ret;
   }

   @Override
   public String toString() {
      return "StatsCollectingCache '" + getName() + "'";
   }
}
