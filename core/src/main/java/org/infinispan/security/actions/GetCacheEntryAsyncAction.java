package org.infinispan.security.actions;

import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.container.entries.CacheEntry;

/**
 * GetCacheEntryAction.
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
public class GetCacheEntryAsyncAction<K, V> extends AbstractAdvancedCacheAction<CompletionStage<CacheEntry<K,V>>> {

   private final K key;

   public GetCacheEntryAsyncAction(AdvancedCache<?, ?> cache, K key) {
      super(cache);
      this.key = key;
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> get() {
      return ((AdvancedCache<K, V>) cache).getCacheEntryAsync(key);
   }
}
