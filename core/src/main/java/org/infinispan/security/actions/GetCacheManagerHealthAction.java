package org.infinispan.security.actions;

import org.infinispan.health.Health;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * @since 10.0
 */
public class GetCacheManagerHealthAction extends AbstractEmbeddedCacheManagerAction<Health> {
   public GetCacheManagerHealthAction(EmbeddedCacheManager cacheManager) {
      super(cacheManager);
   }

   @Override
   public Health get() {
      return cacheManager.getHealth();
   }
}
