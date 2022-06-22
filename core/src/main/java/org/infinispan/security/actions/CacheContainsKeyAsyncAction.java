package org.infinispan.security.actions;

import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;

/**
 * @since 14.0
 */
public class CacheContainsKeyAsyncAction<K> extends AbstractAdvancedCacheAction<CompletionStage<Boolean>> {

   private final K key;

   public CacheContainsKeyAsyncAction(AdvancedCache<?, ?> cache, K key) {
      super(cache);
      this.key = key;
   }

   @Override
   public CompletionStage<Boolean> run() {
      return ((AdvancedCache<K, ?>) cache).containsKeyAsync(key);
   }
}
