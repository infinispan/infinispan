package org.jboss.seam.infinispan.test.cacheManager.external;

import javax.enterprise.inject.Specializes;

import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jboss.seam.infinispan.CacheContainerManager;

@Specializes
public class ExternalCacheContainerManager extends CacheContainerManager {

   private static final CacheContainer CACHE_CONTAINER;

   static {
      EmbeddedCacheManager cacheManager = new DefaultCacheManager();

      // define large configuration
      Configuration largeConfiguration = new Configuration();
      largeConfiguration.fluent()
            .eviction()
            .maxEntries(100);

      cacheManager.defineConfiguration("large", largeConfiguration);

      // define quick configuration
      Configuration quickConfiguration = new Configuration();
      quickConfiguration.fluent()
            .eviction()
            .wakeUpInterval(1l);

      cacheManager.defineConfiguration("quick", quickConfiguration);

      CACHE_CONTAINER = cacheManager;
   }

   public ExternalCacheContainerManager() {
      super(CACHE_CONTAINER);
   }
}
