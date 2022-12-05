package org.infinispan.security.actions;

import org.infinispan.AdvancedCache;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * GetEmbeddedCacheManagerAction.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public class GetEmbeddedCacheManagerAction extends AbstractAdvancedCacheAction<EmbeddedCacheManager> {

   public GetEmbeddedCacheManagerAction(AdvancedCache<?, ?> cache) {
      super(cache);
   }

   @Override
   public EmbeddedCacheManager get() {
      return cache.getCacheManager();
   }
}
