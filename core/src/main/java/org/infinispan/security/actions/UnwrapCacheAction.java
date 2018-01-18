package org.infinispan.security.actions;

import org.infinispan.AdvancedCache;
import org.infinispan.security.impl.SecureCacheImpl;

/**
 * UnwrapCacheAction.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */
public class UnwrapCacheAction<K, V> extends AbstractAdvancedCacheAction<AdvancedCache<K, V>> {

   public UnwrapCacheAction(AdvancedCache<K, V> cache) {
      super(cache);
   }

   @Override
   public AdvancedCache<K, V> run() {
      if (cache instanceof SecureCacheImpl) {
         return ((SecureCacheImpl) cache).getDelegate();
      } else {
         return (AdvancedCache<K, V>) cache;
      }
   }

}
