package org.infinispan.security.actions;

import org.infinispan.AdvancedCache;
import org.infinispan.container.entries.CacheEntry;

/**
 * GetCacheEntryAction.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class GetCacheEntryAction<K, V> extends AbstractAdvancedCacheAction<CacheEntry<K, V>> {

   private final K key;

   public GetCacheEntryAction(AdvancedCache<?, ?> cache, K key) {
      super(cache);
      this.key = key;
   }

   @Override
   public CacheEntry<K, V> run() {
      return ((AdvancedCache<K, V>) cache).getCacheEntry(key);
   }
}
