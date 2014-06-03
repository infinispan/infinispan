package org.infinispan.query.backend;

import org.hibernate.search.infinispan.CacheManagerService;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Simple wrapper to make the Cache ComponentRegistry and the CacheManager available to the
 * services managed by Hibernate Search.
 * 
 * @author Sanne Grinovero
 * @since 7.0
 */
final class InfinispanLoopbackService implements CacheManagerService, ComponentRegistryService {

   private final ComponentRegistry componentRegistry;
   private EmbeddedCacheManager cacheManager;

   public InfinispanLoopbackService(ComponentRegistry cr, EmbeddedCacheManager uninitializedCacheManager) {
      this.componentRegistry = cr;
      this.cacheManager = uninitializedCacheManager;
   }

   @Override
   public ComponentRegistry getComponentRegistry() {
      return componentRegistry;
   }

   public EmbeddedCacheManager getEmbeddedCacheManager() {
      return cacheManager;
   }

}
