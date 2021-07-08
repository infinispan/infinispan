package org.infinispan.security.actions;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.manager.PersistenceManager;

public class GetPersistenceManagerAction extends AbstractEmbeddedCacheManagerAction<PersistenceManager> {

   private final String cacheName;

   public GetPersistenceManagerAction(EmbeddedCacheManager cacheManager, String cacheName) {
      super(cacheManager);
      this.cacheName = cacheName;
   }

   @Override
   public PersistenceManager run() {
      Cache<?, ?> cache = cacheManager.getCache(cacheName);
      return cache.getAdvancedCache().getComponentRegistry().getComponent(PersistenceManager.class);
   }
}
