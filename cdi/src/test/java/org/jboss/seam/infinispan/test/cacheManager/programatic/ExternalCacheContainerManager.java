package org.jboss.seam.infinispan.test.cacheManager.programatic;

import javax.enterprise.inject.Specializes;
import javax.inject.Inject;

import org.infinispan.config.Configuration;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.seam.infinispan.CacheContainerManager;
import org.jboss.seam.infinispan.InfinispanExtension;
import org.jboss.seam.infinispan.event.cachemanager.CacheManagerEventBridge;

@Specializes
public class ExternalCacheContainerManager extends CacheContainerManager {

   private static final EmbeddedCacheManager CACHE_CONTAINER;

   static {
      Configuration defaultConfiguration = new Configuration();
      defaultConfiguration.fluent()
            .eviction()
            .maxEntries(7);

      CACHE_CONTAINER = new DefaultCacheManager(defaultConfiguration);
   }

   // Constructor for proxies only
   protected ExternalCacheContainerManager() {
   }

   @Inject
   public ExternalCacheContainerManager(InfinispanExtension extension, CacheManagerEventBridge eventBridge) {
      super(registerObservers(CACHE_CONTAINER, extension, eventBridge));
   }
}
