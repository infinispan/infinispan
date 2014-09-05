package org.infinispan.security.actions;

import org.infinispan.AdvancedCache;
import org.infinispan.factories.GlobalComponentRegistry;

/**
 * GetCacheGlobalComponentRegistryAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheGlobalComponentRegistryAction extends AbstractAdvancedCacheAction<GlobalComponentRegistry> {

   public GetCacheGlobalComponentRegistryAction(AdvancedCache<?, ?> cache) {
      super(cache);
   }

   @Override
   public GlobalComponentRegistry run() {
      return cache.getCacheManager().getGlobalComponentRegistry();
   }

}
