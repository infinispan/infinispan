package org.infinispan.util;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ForwardingCacheEntry;

/**
 * Wrapper for CacheEntry(s) that can be used to update the cache when it's value is set.
 * @param <K> The key type
 * @param <V> The value type
 */
public class EntryWrapper<K, V> extends ForwardingCacheEntry<K, V> {
   private final Cache<K, V> cache;
   private final CacheEntry<K, V> entry;

   /**
    * Creates a new entry wrapper given the cache and entry. If the entry is itself an EntryWrapper then the inner
    * entry is instead wrapped to prevent double cache.put operations. Also we then we can use the cache given as
    * this could have different flags etc.
    * @param cache the cache to use on setValue
    * @param entry the actual entry
    */
   public EntryWrapper(Cache<K, V> cache, CacheEntry<K, V> entry) {
      this.cache = (Cache<K, V>) cache.getAdvancedCache().withStorageMediaType().withEncoding(IdentityEncoder.class);
      // Don't double wrap, but take the most recent
      if (entry instanceof EntryWrapper) {
         this.entry = ((EntryWrapper<K, V>) entry).entry;
      } else {
         this.entry = entry;
      }
   }

   @Override
   protected CacheEntry<K, V> delegate() {
      return entry;
   }

   @Override
   public V setValue(V value) {
      cache.put(entry.getKey(), value);
      return super.setValue(value);
   }
}
