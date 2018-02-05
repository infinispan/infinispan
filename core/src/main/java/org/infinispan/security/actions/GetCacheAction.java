package org.infinispan.security.actions;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * GetCacheAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetCacheAction extends AbstractEmbeddedCacheManagerAction<Cache<?, ?>> {

   private final String cacheName;

   public GetCacheAction(EmbeddedCacheManager cacheManager, String cacheName) {
      super(cacheManager);
      this.cacheName = cacheName;
   }

   @Override
   public Cache<?, ?> run() {
      return cacheManager.getCache(cacheName);
   }

}
